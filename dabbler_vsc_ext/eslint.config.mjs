import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
    { ignores: ["out/", "node_modules/"] },
    js.configs.recommended,
    tseslint.configs.recommended,
);
