from pycookiecheat import chrome_cookies
import traceback

try:
    cookies = chrome_cookies('https://google.com')
    if "__Secure-1PSID" in cookies:
        print("Success! Extracted __Secure-1PSID:", cookies["__Secure-1PSID"][:10], "...")
    else:
        print("Cookie not found in dictionary.")
except Exception as e:
    traceback.print_exc()
