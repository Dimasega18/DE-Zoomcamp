import sys
import time

def type_writer(text, delay=0.05):
    """
    Simulates a typing effect by printing characters one by one.

    Parameters:
    text (str): Text to display.
    delay (float): Delay between each character.
    """
    for char in text:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(delay)
    
    print()