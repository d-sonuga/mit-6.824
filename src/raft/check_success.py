import os

def check_success(filename):
    with open(filename) as f:
        lines = f.read().splitlines()
        last_line = lines[len(lines) - 1]
        if last_line.startswith("ok"):
            return True
        return False


for filename in os.listdir("."):
    if filename.startswith("test_output-"):
        if not check_success(filename):
            print("Test failed:", filename)

print("Done")

