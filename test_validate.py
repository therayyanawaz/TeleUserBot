import asyncio, os, shutil
from pathlib import Path
from playwright.async_api import async_playwright
from gemini_webapi import GeminiClient

async def run():
    src_dir = Path("~/.config/google-chrome").expanduser()
    dst_dir = Path(".gemini_test_profile").absolute()
    
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
        
    os.makedirs(dst_dir / "Default", exist_ok=True)
    shutil.copy2(src_dir / "Local State", dst_dir / "Local State")
    shutil.copy2(src_dir / "Default" / "Cookies", dst_dir / "Default" / "Cookies")

    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=dst_dir,
            channel="chrome",
            headless=True,
            ignore_default_args=["--use-mock-keychain", "--password-store=basic"]
        )
        cookies = await browser.cookies()
        c_1psid = ""
        c_1psidts = ""
        for c in cookies:
            if c["name"] == "__Secure-1PSID":
                c_1psid = c["value"]
            elif c["name"] == "__Secure-1PSIDTS":
                c_1psidts = c["value"]
        await browser.close()
        
    print(f"Extracted 1PSID: {c_1psid[:10]}...")
    
    # Now try to validate with GeminiClient
    print("Validating with GeminiClient...")
    try:
        client = GeminiClient(c_1psid, c_1psidts)
        await client.init()
        print("Successfully validated!")
    except Exception as e:
        print("Validation failed:", e)

asyncio.run(run())
