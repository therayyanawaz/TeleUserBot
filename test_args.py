import asyncio
from playwright.async_api import async_playwright
async def r():
  async with async_playwright() as p:
      b = await p.chromium.launch_persistent_context('.gemini_test_profile', ignore_default_args=['--non-existent'])
      await b.close()
asyncio.run(r())
