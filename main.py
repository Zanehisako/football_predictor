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
        # # get HTML content of the page as a string
        # content = await page.get_content()
        # # NOTE: Content will now be very long, so avoid printing the whole thing
        # print("Page content length:", len(content)) 
        # scores= await page.select("#content > div.scorebox")
        # texts = scores.text_all.split()
        # print("Scorebox Texts:", texts)

        home_team_name= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
        print("Home team name:", home_team_name.text)
        away_team_name= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
        print("away team name:", away_team_name.text)
        home_team_score= await page.select("#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
        print("Home team score:", home_team_score.text)
        away_team_score= await page.select("#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
        print("away team score:", away_team_score.text)
        home_team_xg= await page.select("#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
        print("Home team XG:", home_team_xg.text)
        away_team_xg= await page.select("#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")
        print("away team XG:", away_team_xg.text)

        home_team_manager= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(5)")
        print("Home team manager:", home_team_manager.text_all.split(":")[-1])
        away_team_manager= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(5)")
        print("away team manager:", away_team_manager.text_all.split(":")[-1])

        home_team_capatin= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
        print("Home team capatin:", home_team_capatin.text)
        away_team_capatin= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")
        print("away team capatin:", away_team_capatin.text)

        match_time= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime")
        print("match time:", match_time.text_all.split()[0])

        attendance= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
        print("attendance:", attendance.text)

        venue= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
        print("venue:", venue.text)

        officials= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small")
        print("officials:", officials.text_all.strip().split("·"))

        home_team_possession = await page.select("#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
        print("Home team possession:", home_team_possession.text)
        away_team_possession = await page.select("#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")
        print("away team possession:", away_team_possession.text)

        home_team_pass_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
        print("Home team pass_accuracy:", home_team_pass_accuracy.text)
        away_team_pass_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")
        print("away team pass_accuracy:", away_team_pass_accuracy.text)

        home_team_shot_accuracy= await page.select("#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
        print("Home team shot_accuracy:", home_team_shot_accuracy.text)
        away_team_shot_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")
        print("away team shot_accuracy:", away_team_shot_accuracy.text)

        home_team_save_accuracy= await page.select("#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
        print("Home team save_accuracy:", home_team_save_accuracy.text)
        away_team_save_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")
        print("away team save_accuracy:", away_team_save_accuracy.text)


        
    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if browser:
            # Explicitly close the browser before exiting the main function
            await browser.stop() 
            print("Browser Stopped.")

if __name__ == "__main__":
    asyncio.run(main())