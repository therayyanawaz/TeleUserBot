import asyncio
from gemini_webapi import GeminiClient

async def run():
    c_1psid = "g.a000_wgiKkrXLomb4rl0Wxn5gtWCXvlf0Zfg1gVH_hF0DaPjxiQ3URqfQBQk14Tte3GP8MwhhAACgYKAQwSARESFQHGX2MiLKgp1TrySUNZEx0aslW98xoVAUF8yKqJxr4dnnYXKZ9BJ_gz0Z1E0076"
    c_1psidts = "sidts-CjIBPWEu2ZvroK2kwU6Ugr17OymWEDH9KXS1tb_70b_t-BZwmZD5CXlTnD51INvI1inKhBAA"
    
    print("Validating with GeminiClient full check...")
    try:
        client = GeminiClient(c_1psid, c_1psidts)
        await client.init()
        res = await client.generate_content("hello")
        print("Successfully validated! Response:", res)
    except Exception as e:
        print("Validation failed:", type(e), e)

asyncio.run(run())
