import os
import re
import subprocess
import sys

def get_numbered_scripts():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    files = os.listdir(current_dir)

    # Filter for .py files that start with a number
    numbered_scripts = []
    pattern = re.compile(r'^(\d+)_.*\.py$')

    for file in files:
        match = pattern.match(file)
        if match:
            num = int(match.group(1))
            numbered_scripts.append((num, file))

    # Sort by the leading number
    numbered_scripts.sort(key=lambda x: x[0])

    return [script[1] for script in numbered_scripts]


def run_scripts(scripts):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    for script in scripts:
        script_path = os.path.join(current_dir, script)
        print(f"\n🚀 Running {script}...")
        result = subprocess.run([sys.executable, script_path])
        if result.returncode != 0:
            print(f"❌ {script} exited with error code {result.returncode}")
            break
        else:
            print(f"✅ {script} completed successfully.")


if __name__ == "__main__":
    scripts = get_numbered_scripts()

    if not scripts:
        print("⚠️ No numbered scripts found.")
        sys.exit(0)

    print(f"Found {len(scripts)} numbered scripts: {scripts}")
    run_scripts(scripts)

