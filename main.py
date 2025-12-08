import asyncio
import zendriver as zd
import pandas as pd
from io import StringIO

async def main():
    browser = None # Initialize to None for the finally block
    try:
        browser = await zd.start(headless=True)
        print("Browser started.")
        
        # 1. Get the page
        page = await browser.get("https://fbref.com/en/matches/d34e407e/Real-Madrid-Osasuna-August-19-2025-La-Liga")
        
        # Wait for the main content to load. A common element on match pages is the box score table.
        # The selector below targets the "Scores & Fixtures" table wrapper.
        await page.wait_for('#content', timeout=20000) 
        await asyncio.sleep(2) # Give it a little extra time to render after dismissal

        print("Loaded URL:", page.url)
        # get HTML content of the page as a string
        content = await page.get_content()
        # NOTE: Content will now be very long, so avoid printing the whole thing
        print("Page content length:", len(content)) 
        scores= await page.select("#content > div.scorebox")
        print("Scores found:", scores.text_all) 

        
    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if browser:
            # Explicitly close the browser before exiting the main function
            await browser.stop() 
            print("Browser Stopped.")

if __name__ == "__main__":
    asyncio.run(main())