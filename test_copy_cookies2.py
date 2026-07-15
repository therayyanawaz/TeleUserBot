import asyncio, os, shutil
from pathlib import Path
from playwright.async_api import async_playwright

async def run():
    src_dir = Path("~/.config/google-chrome").expanduser()
    dst_dir = Path(".gemini_test_profile").absolute()
    
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
        
    os.makedirs(dst_dir / "Default", exist_ok=True)
    
    try:
        shutil.copy2(src_dir / "Local State", dst_dir / "Local State")
        shutil.copy2(src_dir / "Default" / "Cookies", dst_dir / "Default" / "Cookies")
    except Exception as e:
        print("Copy failed:", e)
        return

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=dst_dir,
                channel="chrome",
                headless=True,
                ignore_default_args=["--use-mock-keychain", "--password-store=basic"]
            )
            cookies = await browser.cookies()
            print(f"Found {len(cookies)} cookies!")
            for c in cookies:
                if "1PSID" in c["name"]:
                    print("Found Google cookie:", c["name"])
            await browser.close()
        except Exception as e:
            print(f"Failed: {e}")

asyncio.run(run())
