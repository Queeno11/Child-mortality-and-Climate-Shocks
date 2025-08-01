import subprocess
import sys
from pathlib import Path
import platform

def run_script_in_new_window(executable, script_path):
    """
    Executes a script in a new PowerShell window, waits for it to complete,
    and checks if it was successful.

    Args:
        executable (str): The program to run (e.g., "python", "julia").
        script_path (Path): The path to the script file.
    """
    script_name = script_path.name
    print(f"--- Launching '{script_name}' in a new PowerShell window... ---")
    print("--- Please watch the new window for progress. This script will wait. ---")

    # The command that will be executed inside the new PowerShell window
    command_to_run_in_ps = f"& '{executable}' '{script_path}'"

    # A small, self-contained PowerShell script that we will run.
    # This script does several important things:
    # 1. Sets a unique window title.
    # 2. Runs our actual Python/Julia command.
    # 3. Checks the exit code ($LASTEXITCODE) of our command.
    # 4. Prints a clear success or failure message IN THE NEW WINDOW.
    # 5. Waits for the user to press a key before closing the window.
    # 6. Exits PowerShell with the SAME exit code as our script, which is
    #    the key to telling our main Python script whether it succeeded or failed.
    powershell_script_block = f"""
    $Host.UI.RawUI.WindowTitle = 'Running: {script_name}';
    Write-Host '--- Executing command: {command_to_run_in_ps} ---';
    
    {command_to_run_in_ps};

    if ($LASTEXITCODE -eq 0) {{
        Write-Host "`n--- Script finished successfully. Press any key to close this window... ---" -ForegroundColor Green;
    }} else {{
        Write-Host "`n!!! SCRIPT FAILED with exit code $LASTEXITCODE. Press any key to close this window... !!!" -ForegroundColor Red;
    }}

    $Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown') | Out-Null;
    exit $LASTEXITCODE
    """
    
    try:
        # 'subprocess.run' will block and wait for the PowerShell process to complete.
        # 'check=True' will raise an error if the PowerShell process returns a non-zero exit code.
        subprocess.run(
            ["powershell", "-Command", powershell_script_block],
            check=True
        )
        print(f"--- '{script_name}' completed successfully. ---\n")

    except FileNotFoundError:
        print("ERROR: 'powershell.exe' not found. This script is designed for Windows.")
        sys.exit(1)
        
    except subprocess.CalledProcessError as e:
        # This block runs if the PowerShell script exits with a non-zero code.
        print(f"\n!!! SCRIPT FAILED: '{script_name}' reported an error. !!!")
        print(f"--- Pipeline halted. Check the PowerShell window for error details. ---")
        sys.exit(1)

def main():
    """Main function to define and run the pipeline."""
    if platform.system() != "Windows":
        print("This script is configured to use PowerShell and is intended for Windows.")
        sys.exit(1)

    pipeline_dir = Path(__file__).parent.resolve()

    scripts_to_run = [
        {"exec": "python", "path": pipeline_dir / "02_assign_shocks_to_DHS.py"},
        {"exec": "python", "path": pipeline_dir / "03_merge_climate_and_DHS.py"},
        {"exec": "julia",  "path": pipeline_dir / "04_regressions.jl"}
    ]

    print("=== Starting Data Processing Pipeline ===\n")
    for script_info in scripts_to_run:
        run_script_in_new_window(script_info["exec"], script_info["path"])
    
    print("=== All scripts completed successfully! ===")

if __name__ == "__main__":
    main()