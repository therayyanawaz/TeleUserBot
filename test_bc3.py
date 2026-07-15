import browser_cookie3
import traceback

try:
    cj = browser_cookie3.chrome(domain_name='google.com')
    for c in cj:
        if c.name == "__Secure-1PSID":
            print(f"Found! {c.value[:10]}...")
except Exception as e:
    traceback.print_exc()
