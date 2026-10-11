REM publishes the version in package.json; bump it with "python version.py" and commit first
npm run compile && npm run lint && npx vsce publish
