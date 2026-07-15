import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        try:
            print("Trying channel='chrome'...")
            browser = await p.chromium.launch(channel="chrome", headless=True)
            print("Chrome launched successfully!")
            await browser.close()
        except Exception as e:
            print(f"Failed to launch Chrome: {e}")

asyncio.run(run())
