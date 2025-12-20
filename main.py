import asyncio
import zendriver as zd
import pandas as pd
import random
import os
import zendriver.cdp.network as network # Import the network module

# --- Helper Function for Safe Extraction ---
async def safe_get(page, selector, attribute='text', timeout=1):
    """Safely gets an attribute from an element, returning None on failure."""
    try:
        # Reduced timeout slightly for optional stats to speed up scraping
        element = await page.select(selector, timeout=timeout)
        if not element:
            return None
            
        if attribute == 'text':
            return element.text
        elif attribute == 'text_all':
            return element.text_all
        elif attribute == 'child_count':
            return element.child_node_count
        else:
            return element 
    except Exception:
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
            
            # --- BASIC INFO ---
            home_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            away_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            home_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            away_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            home_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            away_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")

            # Managers & Captains
            h_mgr_full = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(5)", 'text_all')
            a_mgr_full = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(5)", 'text_all')
            home_team_manager = h_mgr_full.split(":")[-1].strip() if h_mgr_full else None
            away_team_manager = a_mgr_full.split(":")[-1].strip() if a_mgr_full else None

            home_team_captain = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
            away_team_captain = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")

            # Meta Data
            match_time_full = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime", 'text_all')
            match_time = match_time_full.split()[0] if match_time_full else None
            attendance = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
            venue = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
            officials = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small", 'text_all')

            # --- TEAM STATS TABLE (Possession, Cards, Accuracy) ---
            home_team_possession = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_possession = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            # New Accuracy Stats
            home_pass_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_pass_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            home_shot_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_shot_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            home_save_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_save_acc = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_cards = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div", 'child_count')
            away_team_cards = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div", 'child_count')

            # --- EXTRA STATS (Fouls, Corners, etc.) ---
            # Row 1
            h_fouls = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(4)")
            a_fouls = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(6)")
            h_corners = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(7)")
            a_corners = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(9)")
            h_crosses = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(10)")
            a_crosses = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(12)")
            h_touches = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(13)")
            a_touches = await safe_get(page, "#team_stats_extra > div:nth-child(1) > div:nth-child(15)")

            # Row 2
            h_tackles = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(4)")
            a_tackles = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(6)")
            h_interceptions = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(7)")
            a_interceptions = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(9)")
            h_aerials = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(10)")
            a_aerials = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(12)")
            h_clearances = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(13)")
            a_clearances = await safe_get(page, "#team_stats_extra > div:nth-child(2) > div:nth-child(15)")

            # Row 3
            h_offsides = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(4)")
            a_offsides = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(6)")
            h_goalkicks = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(7)")
            a_goalkicks = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(9)")
            h_throwins = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(10)")
            a_throwins = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(12)")
            h_longballs = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(13)")
            a_longballs = await safe_get(page, "#team_stats_extra > div:nth-child(3) > div:nth-child(15)")

            print(f"   Success on attempt {attempt}")
            
            return {
                'match_url': url,
                'home_team_name': home_team_name,
                'away_team_name': away_team_name,
                'home_team_score': home_team_score,
                'away_team_score': away_team_score,
                'home_team_xg': home_team_xg,
                'away_team_xg': away_team_xg,
                'home_team_manager': home_team_manager,
                'away_team_manager': away_team_manager,
                'home_team_captain': home_team_captain,
                'away_team_captain': away_team_captain,
                'match_time': match_time,
                'attendance': attendance,
                'venue': venue,
                'officials': officials,
                'home_team_possession': home_team_possession,
                'away_team_possession': away_team_possession,
                'home_team_cards_number': home_team_cards,
                'away_team_cards_number': away_team_cards,
                # New Stats
                'home_pass_accuracy': home_pass_acc,
                'away_pass_accuracy': away_pass_acc,
                'home_shot_accuracy': home_shot_acc,
                'away_shot_accuracy': away_shot_acc,
                'home_save_accuracy': home_save_acc,
                'away_save_accuracy': away_save_acc,
                'home_fouls': h_fouls,
                'away_fouls': a_fouls,
                'home_corners': h_corners,
                'away_corners': a_corners,
                'home_crosses': h_crosses,
                'away_crosses': a_crosses,
                'home_touches': h_touches,
                'away_touches': a_touches,
                'home_tackles': h_tackles,
                'away_tackles': a_tackles,
                'home_interceptions': h_interceptions,
                'away_interceptions': a_interceptions,
                'home_aerials_won': h_aerials,
                'away_aerials_won': a_aerials,
                'home_clearances': h_clearances,
                'away_clearances': a_clearances,
                'home_offsides': h_offsides,
                'away_offsides': a_offsides,
                'home_goal_kicks': h_goalkicks,
                'away_goal_kicks': a_goalkicks,
                'home_throw_ins': h_throwins,
                'away_throw_ins': a_throwins,
                'home_long_balls': h_longballs,
                'away_long_balls': a_longballs
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
        club_urls = pd.read_csv("club_urls.csv")['club_url'].tolist()
        
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