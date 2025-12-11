import asyncio
import zendriver as zd
import pandas as pd
from io import StringIO

async def get_page_content(url,browser):
        print("Starting to scrape the page:", url)
        # 1. Get the page
        page = await browser.get(url)
        
        # Wait for the main content to load. A common element on match pages is the box score table.
        # The selector below targets the "Scores & Fixtures" table wrapper.
        await page.wait_for('#content', timeout=20000) 
        await asyncio.sleep(2) # Give it a little extra time to render after dismissal

        #print("Loaded URL:", page.url)
        # # get HTML content of the page as a string
        # content = await page.get_content()
        # # NOTE: Content will now be very long, so avoid #printing the whole thing
        # #print("Page content length:", len(content)) 
        # scores= await page.select("#content > div.scorebox")
        # texts = scores.text_all.split()
        # #print("Scorebox Texts:", texts)

        home_team_name= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
        #print("Home team name:", home_team_name.text)
        away_team_name= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
        #print("away team name:", away_team_name.text)
        home_team_score= await page.select("#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
        #print("Home team score:", home_team_score.text)
        away_team_score= await page.select("#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
        #print("away team score:", away_team_score.text)
        home_team_xg= await page.select("#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
        #print("Home team XG:", home_team_xg.text)
        away_team_xg= await page.select("#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")
        #print("away team XG:", away_team_xg.text)

        home_team_manager= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(5)")
        #print("Home team manager:", home_team_manager.text_all.split(":")[-1])
        away_team_manager= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(5)")
        #print("away team manager:", away_team_manager.text_all.split(":")[-1])

        home_team_capatin= await page.select("#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
        #print("Home team capatin:", home_team_capatin.text)
        away_team_capatin= await page.select("#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")
        #print("away team capatin:", away_team_capatin.text)

        match_time= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime")
        #print("match time:", match_time.text_all.split()[0])

        attendance= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
        #print("attendance:", attendance.text)

        venue= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
        #print("venue:", venue.text)

        officials= await page.select("#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small")
        #print("officials:", officials.text_all.strip().split("·"))

        home_team_possession = await page.select("#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
        #print("Home team possession:", home_team_possession.text)
        away_team_possession = await page.select("#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")
        #print("away team possession:", away_team_possession.text)

        home_team_pass_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
        #print("Home team pass_accuracy:", home_team_pass_accuracy.text)
        away_team_pass_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")
        #print("away team pass_accuracy:", away_team_pass_accuracy.text)

        home_team_shot_accuracy= await page.select("#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
        #print("Home team shot_accuracy:", home_team_shot_accuracy.text)
        away_team_shot_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")
        #print("away team shot_accuracy:", away_team_shot_accuracy.text)

        home_team_save_accuracy= await page.select("#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
        #print("Home team save_accuracy:", home_team_save_accuracy.text)
        away_team_save_accuracy = await page.select("#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")
        #print("away team save_accuracy:", away_team_save_accuracy.text)

        home_team_cards= await page.select("#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div")
        home_team_cards_number =  home_team_cards.child_node_count
        #print("Home team cards:", home_team_cards_number)
        away_team_cards = await page.select("#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div")
        away_team_cards_number =  away_team_cards.child_node_count
        #print("away team cards:", away_team_cards_number)

        home_team_fouls= await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(4)")
        #print("Home team fouls:", home_team_fouls.text)
        away_team_fouls = await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(6)")
        #print("away team fouls:", away_team_fouls.text)

        home_team_corners= await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(7)")
        #print("Home team corners:", home_team_corners.text)
        away_team_corners = await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(9)")
        #print("away team corners:", away_team_corners.text)

        home_team_crosses= await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(10)")
        #print("Home team crosses:", home_team_crosses.text)
        away_team_crosses = await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(12)")
        #print("away team crosses:", away_team_crosses.text)

        home_team_touches= await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(13)")
        #print("Home team touches:", home_team_touches.text)
        away_team_touches = await page.select("#team_stats_extra > div:nth-child(1) > div:nth-child(15)")
        #print("away team touches:", away_team_touches.text)

        home_team_tackels= await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(4)")
        #print("Home team tackels:", home_team_tackels.text)
        away_team_tackels = await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(6)")
        #print("away team tackels:", away_team_tackels.text)

        home_team_interceptions= await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(7)")
        #print("Home team interceptions:", home_team_interceptions.text)
        away_team_interceptions = await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(9)")
        #print("away team interceptions:", away_team_interceptions.text)

        home_team_aerials= await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(10)")
        #print("Home team aerials:", home_team_aerials.text)
        away_team_aerials = await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(12)")
        #print("away team aerials:", away_team_aerials.text)

        home_team_clearances= await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(13)")
        #print("Home team clearances:", home_team_clearances.text)
        away_team_clearances = await page.select("#team_stats_extra > div:nth-child(2) > div:nth-child(15)")
        #print("away team clearances:", away_team_clearances.text)

        home_team_offsides= await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(4)")
        #print("Home team offsides:", home_team_offsides.text)
        away_team_offsides = await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(6)")
        #print("away team offsides:", away_team_offsides.text)

        home_team_goal_kicks= await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(7)")
        #print("Home team goal_kicks:", home_team_goal_kicks.text)
        away_team_goal_kicks = await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(9)")
        #print("away team goal_kicks:", away_team_goal_kicks.text)

        home_team_throw_ins= await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(10)")
        #print("Home team throw_ins:", home_team_throw_ins.text)
        away_team_throw_ins = await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(12)")
        #print("away team throw_ins:", away_team_throw_ins.text)

        home_team_long_balls= await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(13)")
        #print("Home team long_balls:", home_team_long_balls.text)
        away_team_long_balls = await page.select("#team_stats_extra > div:nth-child(3) > div:nth-child(15)")
        #print("away team long_balls:", away_team_long_balls.text)
        print("finished scrapping the page:", url)
        return {
            'home_team_name': home_team_name.text,
            'away_team_name': away_team_name.text,
            'home_team_score': home_team_score.text,
            'away_team_score': away_team_score.text,
            'home_team_xg': home_team_xg.text,
            'away_team_xg': away_team_xg.text,
            'home_team_manager': home_team_manager.text_all.split(":")[-1],
            'away_team_manager': away_team_manager.text_all.split(":")[-1],
            'home_team_captain': home_team_capatin.text,
            'away_team_captain': away_team_capatin.text,
            'match_time': match_time.text_all.split()[0],
            'attendance': attendance.text,
            'venue': venue.text,
            'officials': officials.text_all,
            'home_team_possession': home_team_possession.text,
            'away_team_possession': away_team_possession.text,
            'home_team_pass_accuracy': home_team_pass_accuracy.text,
            'away_team_pass_accuracy': away_team_pass_accuracy.text,
            'home_team_shot_accuracy': home_team_shot_accuracy.text,
            'away_team_shot_accuracy': away_team_shot_accuracy.text,
            'home_team_save_accuracy': home_team_save_accuracy.text,
            'away_team_save_accuracy': away_team_save_accuracy.text,
            'home_team_cards_number': home_team_cards_number,
            'away_team_cards_number': away_team_cards_number,
            'home_team_fouls': home_team_fouls.text,
            'away_team_fouls': away_team_fouls.text,
            'home_team_corners': home_team_corners.text,
            'away_team_corners': away_team_corners.text,
            'home_team_crosses': home_team_crosses.text,
            'away_team_crosses': away_team_crosses.text,
            'home_team_touches': home_team_touches.text,
            'away_team_touches': away_team_touches.text,
            'home_team_tackels': home_team_tackels.text,
            'away_team_tackels': away_team_tackels.text,
            'home_team_interceptions': home_team_interceptions.text,
            'away_team_interceptions': away_team_interceptions.text,
            'home_team_aerials': home_team_aerials.text,
            'away_team_aerials': away_team_aerials.text,
            'home_team_clearances': home_team_clearances.text,
            'away_team_clearances': away_team_clearances.text,
            'home_team_offsides': home_team_offsides.text,
            'away_team_offsides': away_team_offsides.text,
            'home_team_goal_kicks': home_team_goal_kicks.text,
            'away_team_goal_kicks': away_team_goal_kicks.text,
            'home_team_throw_ins': home_team_throw_ins.text,
            'away_team_throw_ins': away_team_throw_ins.text,
            'home_team_long_balls': home_team_long_balls.text,
            'away_team_long_balls': away_team_long_balls.text
        }
        


async def main():
    browser = None # Initialize to None for the finally block
    try:

        df = pd.DataFrame()
        browser = await zd.start(headless=True)
        urls = ["https://fbref.com/en/matches/d34e407e/Real-Madrid-Osasuna-August-19-2025-La-Liga","https://fbref.com/en/matches/fde70dd0/Oviedo-Real-Madrid-August-24-2025-La-Liga","https://fbref.com/en/matches/9c0a49c5/Real-Madrid-Mallorca-August-30-2025-La-Liga"]
        for url in urls:
            result =  await get_page_content(url, browser)
            df = pd.concat([df, pd.DataFrame([result])], ignore_index=True)
        
        print("DataFrame created successfully!")
        print(df)
        df.to_csv("match_data.csv", index=False)
        print(".csv file created successfully!")
        print("Browser started.")
        


        
    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if browser:
            # Explicitly close the browser before exiting the main function
            await browser.stop() 
            #print("Browser Stopped.")

if __name__ == "__main__":
    asyncio.run(main())