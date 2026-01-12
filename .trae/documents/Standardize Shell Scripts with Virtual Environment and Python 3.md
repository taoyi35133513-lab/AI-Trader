I will modify all 12 shell scripts in the `scripts/` directory to enforce virtual environment activation and use `python3`.

**Modifications for each script:**

1.  **Inject Virtual Environment Activation:**
    Insert the following code block immediately after navigating to the project root (`cd "$PROJECT_ROOT"`):
    ```bash
    # Check and activate virtual environment
    if [ -d ".venv" ]; then
        echo "🔌 Activating virtual environment..."
        source .venv/bin/activate
    else
        echo "❌ Error: Virtual environment .venv not found in project root."
        exit 1
    fi
    ```

2.  **Replace Python Interpreter:**
    - Replace all instances of `python ` with `python3 `.
    - Update `scripts/regenerate_cache.sh` to remove the complex legacy Python detection logic and use the standardized virtual environment `python3`.

**Target Files:**
- `scripts/main.sh`
- `scripts/main_step1.sh`, `scripts/main_step2.sh`, `scripts/main_step3.sh`
- `scripts/main_a_stock_step1.sh`, `scripts/main_a_stock_step2.sh`, `scripts/main_a_stock_step3.sh`
- `scripts/main_crypto_step1.sh`, `scripts/main_crypto_step2.sh`, `scripts/main_crypto_step3.sh`
- `scripts/start_ui.sh`
- `scripts/regenerate_cache.sh`

**Verification Plan:**
After applying the changes, I will run a validation command to ensure:
1.  All scripts contain the virtual environment activation block.
2.  No scripts contain the bare `python` command (except potentially in comments or printed strings, though I will aim to update those too for consistency).
3.  All scripts use `python3`.
