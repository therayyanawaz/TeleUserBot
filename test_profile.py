import asyncio, os
from playwright.async_api import async_playwright

async def run():
    profile_dir = os.path.expanduser("~/.config/google-chrome")
    async with async_playwright() as p:
        try:
            print(f"Trying to launch with profile {profile_dir}...")
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                channel="chrome",
                headless=False
            )
            print("Launched successfully!")
            await browser.close()
        except Exception as e:
            print(f"Failed: {e}")

asyncio.run(run())
