/* -------------------------------------------------------------------------
 * Based on the pygls VS Code client example.
 * Original work Copyright (c) Microsoft Corporation. All rights reserved.
 * Original work licensed under the MIT License.
 * All modifications Copyright (c) Open Law Library. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 * ----------------------------------------------------------------------- */
"use strict";

import { execFile } from "child_process";
import * as vscode from "vscode";
import * as semver from "semver";

import { PythonExtension } from "@vscode/python-extension";
import { LanguageClient, LanguageClientOptions, ServerOptions, State } from "vscode-languageclient/node";

// dabbler finds SQL in f-strings with the tokenizer of Python 3.12+
const MIN_PYTHON = semver.parse("3.12.0")

let client: LanguageClient | undefined;
let clientStarting = false
let python: PythonExtension | undefined;
let logger: vscode.LogOutputChannel


/**
 * Called when VS Code activates the extension (when a python file is opened).
 */
export async function activate(context: vscode.ExtensionContext) {
    logger = vscode.window.createOutputChannel('dabbler', { log: true })
    logger.info("Extension activated.")

    await getPythonExtension();
    if (!python) {
        return
    }

    context.subscriptions.push(
        vscode.commands.registerCommand("dabbler.server.restart", async () => {
            logger.info('restarting server...')
            await startLangServer()
        })
    )

    context.subscriptions.push(
        vscode.commands.registerCommand("dabbler.server.sendSqltoDbDabblerCommand", async () => {
            await runServerCommandAtCursor('sendSqlToDbDabbler')
        })
    )

    context.subscriptions.push(
        vscode.commands.registerCommand("dabbler.server.dbDabblerFormatCurrentStatement", async () => {
            await runServerCommandAtCursor('dbDabblerFormatCurrentStatement')
        })
    )

    context.subscriptions.push(
        vscode.commands.registerCommand("dabbler.server.executeCommand", async () => {
            await executeServerCommand()
        })
    )

    // Restart the language server if the user switches Python envs...
    context.subscriptions.push(
        python.environments.onDidChangeActiveEnvironmentPath(async () => {
            logger.info('python env modified, restarting server...')
            await startLangServer()
        })
    )

    // ... or if they change a relevant config option
    context.subscriptions.push(
        vscode.workspace.onDidChangeConfiguration(async (event) => {
            if (event.affectsConfiguration("dabbler.server") || event.affectsConfiguration("dabbler.client")) {
                logger.info('config modified, restarting server...')
                await startLangServer()
            }
        })
    )

    // Start now: activation happens because a python file is open (including
    // editors restored when VS Code starts, which fire no open events).
    await startLangServer()
}

export function deactivate(): Thenable<void> {
    return stopLangServer()
}

/**
 * Start (or restart) the language server.
 */
async function startLangServer() {
    // Don't interfere if we are already in the process of launching the server.
    if (clientStarting) {
        return
    }
    clientStarting = true
    try {
        if (client) {
            await stopLangServer()
        }

        const pythonPath = await getPythonPath()
        if (!pythonPath) {
            return
        }
        if (!await dabblerIsInstalled(pythonPath)) {
            await offerToInstall(pythonPath)
            return
        }

        logger.info(`launching server: '${pythonPath}' -m dabbler.lsp`)
        const serverOptions: ServerOptions = {
            command: pythonPath,
            args: ["-m", "dabbler.lsp"],
        };
        client = new LanguageClient('dabbler', serverOptions, getClientOptions());
        await client.start()
        const info = client.initializeResult?.serverInfo
        logger.info(`server started: ${info?.name ?? 'dabbler'} ${info?.version ?? ''}`)
    } catch (err) {
        logger.error(`Unable to start server: ${err}`)
        vscode.window.showErrorMessage(`dabbler: the language server didn't start (${err}). See the dabbler output for details.`)
    } finally {
        clientStarting = false
    }
}

async function stopLangServer(): Promise<void> {
    if (!client) {
        return
    }
    const stopping = client
    client = undefined
    try {
        if (stopping.state === State.Running) {
            await stopping.stop()
        }
    } finally {
        await stopping.dispose()
    }
}

function getClientOptions(): LanguageClientOptions {
    const config = vscode.workspace.getConfiguration('dabbler.client')
    const options = {
        documentSelector: config.get<any>('documentSelector'),
        outputChannel: logger,
        connectionOptions: {
            maxRestartCount: 2, // restart after a crash, but not forever
        },
    };
    logger.info(`client options: ${JSON.stringify(options, undefined, 2)}`)
    return options
}

/**
 * True if `python -c "import dabbler.lsp"` works in the chosen environment.
 */
function dabblerIsInstalled(pythonPath: string): Promise<boolean> {
    return new Promise((resolve) => {
        execFile(pythonPath, ["-c", "import dabbler.lsp"], { timeout: 60000 }, (error, _stdout, stderr) => {
            if (error) {
                logger.warn(`dabbler isn't importable with ${pythonPath}: ${stderr || error.message}`)
            }
            resolve(!error)
        })
    })
}

async function offerToInstall(pythonPath: string) {
    const install = "Install dabbler"
    const choose = "Select Interpreter"
    const answer = await vscode.window.showWarningMessage(
        `dabbler isn't installed in the selected Python environment (${pythonPath}).`,
        install, choose,
    )
    if (answer === install) {
        const terminal = vscode.window.createTerminal("dabbler install")
        terminal.show()
        // PowerShell (VS Code's default on Windows) needs & to run a quoted path
        const run = process.platform === "win32" ? `& "${pythonPath}"` : `"${pythonPath}"`
        terminal.sendText(`${run} -m pip install "dabbler[gui]"`)
        vscode.window.showInformationMessage("Restart the dabbler language server once the install finishes (command: dabbler: Restart Language Server).")
    } else if (answer === choose) {
        await vscode.commands.executeCommand('python.setInterpreter')
    }
}

/**
 * Execute a command provided by the language server.
 */
async function executeServerCommand() {
    if (!client || client.state !== State.Running) {
        await vscode.window.showErrorMessage("There is no language server running.")
        return
    }

    const knownCommands = client.initializeResult?.capabilities.executeCommandProvider?.commands
    if (!knownCommands || knownCommands.length === 0) {
        const info = client.initializeResult?.serverInfo
        const name = info?.name || "Server"
        const version = info?.version || ""

        await vscode.window.showInformationMessage(`${name} ${version} does not implement any commands.`)
        return
    }

    const commandName = await vscode.window.showQuickPick(knownCommands, { canPickMany: false })
    if (!commandName) {
        return
    }
    logger.info(`executing command: '${commandName}'`)

    const result = await vscode.commands.executeCommand(commandName)
    logger.info(`${commandName} result: ${JSON.stringify(result, undefined, 2)}`)
}

/**
 * Runs a server command with the active editor's document and cursor position.
 */
async function runServerCommandAtCursor(commandName: string) {
    if (!client || client.state !== State.Running) {
        await vscode.window.showErrorMessage("There is no language server running.")
        return
    }
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        return
    }
    const cursorPosition = editor.selection.active;
    const documentUri = editor.document.uri;
    logger.info(`executing command: '${commandName}' at ${documentUri.toString()} ${cursorPosition.line + 1}:${cursorPosition.character + 1}`)
    const result = await vscode.commands.executeCommand(commandName, documentUri, cursorPosition)
    logger.info(`${commandName} result: ${JSON.stringify(result, undefined, 2)}`)
}

function getDefaultPyEnv(): string | undefined {
    const config = vscode.workspace.getConfiguration("dabbler.server")
    return config.get<string>('default_py_env') || undefined
}

/**
 * The python interpreter to launch the server with: the dabbler.server.default_py_env
 * setting, or the environment selected in the Python extension.
 */
async function getPythonPath(): Promise<string | undefined> {
    if (!python) {
        return
    }

    const defaultPyEnv = getDefaultPyEnv()
    if (defaultPyEnv) {
        logger.info(`Using default environment: ${defaultPyEnv}`)
        return defaultPyEnv
    }

    const activeEnvPath = python.environments.getActiveEnvironmentPath()
    logger.info(`Using environment: ${activeEnvPath.id}: ${activeEnvPath.path}`)

    const activeEnv = await python.environments.resolveEnvironment(activeEnvPath)
    if (!activeEnv) {
        logger.error(`Unable to resolve environment: ${activeEnvPath.path}`)
        return
    }

    const v = activeEnv.version
    const pythonVersion = v ? semver.parse(`${v.major}.${v.minor}.${v.micro}`) : null
    if (pythonVersion && MIN_PYTHON && semver.lt(pythonVersion, MIN_PYTHON)) {
        const response = await vscode.window.showErrorMessage(
            `The selected Python is ${pythonVersion}, but dabbler needs ${MIN_PYTHON} or newer.`,
            "Change Environment",
        )
        if (response) {
            await vscode.commands.executeCommand('python.setInterpreter')
        }
        return
    }

    const pythonUri = activeEnv.executable.uri
    if (!pythonUri) {
        logger.error(`URI of Python executable is undefined!`)
        return
    }

    return pythonUri.fsPath
}

async function getPythonExtension() {
    try {
        python = await PythonExtension.api();
    } catch (err) {
        logger.error(`Unable to load python extension: ${err}`)
    }
}
