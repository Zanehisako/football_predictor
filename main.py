import asyncio
import zendriver as zd
import pandas as pd
import random
import os
import zendriver.cdp.network as network # Import the network module

# --- Helper Function for Safe Extraction ---
async def safe_get(page,selector, timeout=1):
    """Safely gets an attribute from an element, returning None on failure."""
    try:
        # Reduced timeout slightly for optional stats to speed up scraping
        element = await page.select(selector, timeout=timeout)
        return element 

    except Exception:
        print(f"not an element for this selector:{selector}")
        return None

async def get_page_content(url, page):
    print(f"Starting to scrape: {url}")
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            await page.get(url)
            
            # Smart wait
            try:
                await page.wait_for_ready_state("complete", timeout=15)
            except Exception:
                pass 

            # Wait for the scorebox. If this fails, the page isn't ready.
            await page.wait_for('#content > div.scorebox', timeout=10)
            
            home_team_name= await safe_get(page,"#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            #print("Home team name:", home_team_name.text)
            away_team_name= await safe_get(page,"#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            #print("away team name:", away_team_name.text)
            home_team_score= await safe_get(page,"#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            #print("Home team score:", home_team_score.text)
            away_team_score= await safe_get(page,"#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            #print("away team score:", away_team_score.text)
            home_team_xg= await safe_get(page,"#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            #print("Home team XG:", home_team_xg.text)
            away_team_xg= await safe_get(page,"#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")
            #print("away team XG:", away_team_xg.text)

            home_team_manager= await safe_get(page,"#content > div.scorebox > div:nth-child(1) > div:nth-child(5)")
            #print("Home team manager:", home_team_manager.text_all.split(":")[-1])
            away_team_manager= await safe_get(page,"#content > div.scorebox > div:nth-child(2) > div:nth-child(5)")
            #print("away team manager:", away_team_manager.text_all.split(":")[-1])

            "#sb_team_0 > div:nth-child(5) > a"
            "#sb_team_0 > div:nth-child(6) > a"
            home_captin = await page.find("Captain",best_match=True)
            print(f"home_captin{home_captin}")
            # Get the parent element  
            parent = home_captin.parent  
            home_team_capatin= await parent.query_selector("a")  
            if home_team_capatin:  
                player_name = home_team_capatin.text  
                print(player_name)  # Output: "Federico Valverde"

            away_team_capatin= await safe_get(page,"#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")
            #print("away team capatin:", away_team_capatin.text)

            match_time= await safe_get(page,"#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.venuetime")
            #print("match time:", match_time.text_all.split()[0])

            attendance= await safe_get(page,"#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
            #print("attendance:", attendance.text)

            venue= await safe_get(page,"#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
            #print("venue:", venue.text)

            officials= await safe_get(page,"#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small")
            #print("officials:", officials.text_all.strip().split("·"))

            home_team_possession = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
            #print("Home team possession:", home_team_possession.text)
            away_team_possession = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")
            #print("away team possession:", away_team_possession.text)

            home_team_pass_accuracy = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
            #print("Home team pass_accuracy:", home_team_pass_accuracy.text)
            away_team_pass_accuracy = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")
            #print("away team pass_accuracy:", away_team_pass_accuracy.text)

            home_team_shot_accuracy= await safe_get(page,"#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
            #print("Home team shot_accuracy:", home_team_shot_accuracy.text)
            away_team_shot_accuracy = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")
            #print("away team shot_accuracy:", away_team_shot_accuracy.text)

            home_team_save_accuracy= await safe_get(page,"#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
            #print("Home team save_accuracy:", home_team_save_accuracy.text)
            away_team_save_accuracy = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")
            #print("away team save_accuracy:", away_team_save_accuracy.text)

            home_team_cards= await safe_get(page,"#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div")
            home_team_cards_number =  home_team_cards.child_node_count
            #print("Home team cards:", home_team_cards_number)
            away_team_cards = await safe_get(page,"#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div")
            away_team_cards_number =  away_team_cards.child_node_count
            #print("away team cards:", away_team_cards_number)

            home_team_fouls= await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(4)")
            #print("Home team fouls:", home_team_fouls.text)
            away_team_fouls = await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(6)")
            #print("away team fouls:", away_team_fouls.text)

            home_team_corners= await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(7)")
            #print("Home team corners:", home_team_corners.text)
            away_team_corners = await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(9)")
            #print("away team corners:", away_team_corners.text)

            home_team_crosses= await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(10)")
            #print("Home team crosses:", home_team_crosses.text)
            away_team_crosses = await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(12)")
            #print("away team crosses:", away_team_crosses.text)

            home_team_touches= await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(13)")
            #print("Home team touches:", home_team_touches.text)
            away_team_touches = await safe_get(page,"#team_stats_extra > div:nth-child(1) > div:nth-child(15)")
            #print("away team touches:", away_team_touches.text)

            home_team_tackels= await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(4)")
            #print("Home team tackels:", home_team_tackels.text)
            away_team_tackels = await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(6)")
            #print("away team tackels:", away_team_tackels.text)

            home_team_interceptions= await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(7)")
            #print("Home team interceptions:", home_team_interceptions.text)
            away_team_interceptions = await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(9)")
            #print("away team interceptions:", away_team_interceptions.text)

            home_team_aerials= await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(10)")
            #print("Home team aerials:", home_team_aerials.text)
            away_team_aerials = await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(12)")
            #print("away team aerials:", away_team_aerials.text)

            home_team_clearances= await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(13)")
            #print("Home team clearances:", home_team_clearances.text)
            away_team_clearances = await safe_get(page,"#team_stats_extra > div:nth-child(2) > div:nth-child(15)")
            #print("away team clearances:", away_team_clearances.text)

            home_team_offsides= await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(4)")
            #print("Home team offsides:", home_team_offsides.text)
            away_team_offsides = await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(6)")
            #print("away team offsides:", away_team_offsides.text)

            home_team_goal_kicks= await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(7)")
            #print("Home team goal_kicks:", home_team_goal_kicks.text)
            away_team_goal_kicks = await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(9)")
            #print("away team goal_kicks:", away_team_goal_kicks.text)

            home_team_throw_ins= await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(10)")
            #print("Home team throw_ins:", home_team_throw_ins.text)
            away_team_throw_ins = await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(12)")
            #print("away team throw_ins:", away_team_throw_ins.text)

            home_team_long_balls= await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(13)")
            #print("Home team long_balls:", home_team_long_balls.text)
            away_team_long_balls = await safe_get(page,"#team_stats_extra > div:nth-child(3) > div:nth-child(15)")
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

        except Exception as e:
            print(f"   Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(3)
            else:
                return None

async def scrape_all_club_matches_urls(url, page):
    print("Getting match list for:", url)
    urls = []
    
    await page.get(url)
    
    # Wait specifically for the matchlogs table
    try:
        await page.wait_for('#matchlogs_for', timeout=15)
    except Exception:
        print(f"Could not find matchlogs table for {url}")
        return []
    
    consecutive_failures = 0
    
    # Increase range to cover long seasons
    for i in range(1, 80):
        try:
            # Look for "Match Report" link
            selector = f"#matchlogs_for > tbody > tr:nth-child({i}) > td[data-stat='match_report'] > a"
            
            # Use very short timeout. We expect this to fail on header rows.
            link_el = await page.select(selector, timeout=0.2)
            
            if link_el and link_el.text == "Match Report":
                full_url = f"https://fbref.com{link_el.get('href')}"
                urls.append(full_url)
                print(f"Found match ({i}): {full_url}")
                consecutive_failures = 0 
            else:
                consecutive_failures += 1
                
        except Exception:
            consecutive_failures += 1
            
        # Only stop if we see 5 non-match rows in a row
        if consecutive_failures >= 5:
            break
            
    print(f"Found {len(urls)} matches.")
    return urls

async def scrape_all_club_matches(urls, page):
    print(f"Starting to scrape {len(urls)} matches...")
    results = []
    
    for i, url in enumerate(urls):
        print(f"\nProcessing {i+1}/{len(urls)}")
        try:
            match_data = await get_page_content(url, page)
            if match_data:
                results.append(match_data)
            
            # Polite sleep to avoid IP bans
            await asyncio.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"Error on {url}: {e}")
            continue

    return pd.DataFrame(results)

async def main():
    browser = None
    csv_file = "match_data.csv"
    
    # 1. Load existing data to avoid duplicates
    existing_urls = set()
    df_old = pd.DataFrame()
    
    if os.path.exists(csv_file):
        try:
            df_old = pd.read_csv(csv_file)
            print(f"Loaded existing data with {len(df_old)} rows.")
            if 'match_url' in df_old.columns:
                existing_urls = set(df_old['match_url'].unique())
        except Exception as e:
            print(f"Error reading CSV: {e}")

    try:
        # Load the input file containing club stats pages
        # Ensure you have a file named 'club_urls.csv' with a column 'club_url'
        club_urls = pd.read_csv("club_urls_test.csv")['club_url'].tolist()
        
        browser = await zd.start(headless=True)
        page = await browser.get("about:blank")

        # --- OPTIMIZATION: ENABLE NETWORK BLOCKING ---
        # 1. Enable network tracking
        await page.send(network.enable())
        
        # 2. Block heavy resources
        # Note the function name is 'set_blocked_ur_ls' not 'set_blocked_urls'
        await page.send(network.set_blocked_ur_ls(urls=[
            "*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg", "*.webp", # Images
            "*.css",                                                # Styles (layouts)
            "*.woff", "*.woff2",                                    # Fonts
            "*doubleclick*", "*google-analytics*",                  # Ads/Tracking
            "*googlesyndication*", "*adservice*",
            "*facebook*", "*twitter*", "*youtube*"
        ]))
        print(f"   [Worker] Network blocking enabled (No images/ads)")
        # ---------------------------------------------
        
        # 2. Collect ALL potential URLs
        all_found_urls = []
        for url in club_urls:
            found = await scrape_all_club_matches_urls(url, page)
            all_found_urls.extend(found)
        
        unique_found_urls = list(set(all_found_urls))
        print(f"Total unique URLs found: {len(unique_found_urls)}")

        # 3. Filter out URLs already in the CSV
        urls_to_scrape = [u for u in unique_found_urls if u not in existing_urls]
        
        if not urls_to_scrape:
            print("No new matches found to scrape.")
        else:
            print(f"New matches to scrape: {len(urls_to_scrape)}")
            df_new = await scrape_all_club_matches(urls_to_scrape, page)
            
            if not df_new.empty:
                # Combine old and new data
                df_final = pd.concat([df_old, df_new], ignore_index=True)
                
                # Double check for duplicates
                if 'match_url' in df_final.columns:
                    df_final = df_final.drop_duplicates(subset=['match_url'])
                
                # Save to CSV (This will add the new columns automatically)
                df_final.to_csv(csv_file, index=False)
                print(f"Successfully saved {len(df_final)} rows.")
            else:
                print("No data scraped.")
        
    except Exception as e:
        print(f"An error occurred: {e}")


    finally:
        if browser:
            await browser.stop() 
            print("Browser Stopped.")

if __name__ == "__main__":
    asyncio.run(main())