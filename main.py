import asyncio
import zendriver as zd
import pandas as pd
import random
import os

# --- Helper Function for Safe Extraction ---
async def safe_get(page, selector, attribute='text', timeout=2):
    """Safely gets an attribute from an element, returning None on failure."""
    try:
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
            
            # Smart wait: try waiting for complete, but proceed if content loads early
            try:
                await page.wait_for_ready_state("complete", timeout=15)
            except Exception:
                pass 

            # Wait for the scorebox. If this fails, the page isn't ready.
            await page.wait_for('#content > div.scorebox', timeout=10)
            
            # --- Extraction ---
            home_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            away_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            home_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            away_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            home_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            away_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")

            h_mgr_full = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(5)", 'text_all')
            a_mgr_full = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(5)", 'text_all')
            home_team_manager = h_mgr_full.split(":")[-1].strip() if h_mgr_full else None
            away_team_manager = a_mgr_full.split(":")[-1].strip() if a_mgr_full else None

            home_team_captain = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
            away_team_captain = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")

            match_time_full = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime", 'text_all')
            match_time = match_time_full.split()[0] if match_time_full else None

            attendance = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
            venue = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
            officials = await safe_get(page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small", 'text_all')

            home_team_possession = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_possession = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_cards = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div", 'child_count')
            away_team_cards = await safe_get(page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div", 'child_count')

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
    
    # --- MAJOR FIX: TOLERATE GAPS IN TABLE ---
    consecutive_failures = 0
    
    # Increase range to cover long seasons
    for i in range(1, 80):
        try:
            # Better selector: This specifically looks for the "Match Report" column
            # instead of relying on ".left.group_start" which might be missing on some rows
            selector = f"#matchlogs_for > tbody > tr:nth-child({i}) > td[data-stat='match_report'] > a"
            
            # Use very short timeout. We expect this to fail on header rows.
            link_el = await page.select(selector, timeout=0.2)
            
            if link_el and link_el.text == "Match Report":
                full_url = f"https://fbref.com{link_el.get('href')}"
                urls.append(full_url)
                print(f"Found match ({i}): {full_url}")
                consecutive_failures = 0 # Reset counter because we found a match
            else:
                # Row exists but isn't a match (e.g. "Champions League" header)
                consecutive_failures += 1
                
        except Exception:
            # Row probably doesn't exist or timed out
            consecutive_failures += 1
            
        # Only stop if we see 5 non-match rows in a row
        if consecutive_failures >= 5:
            # print(f"Stopping scan at row {i} due to {consecutive_failures} consecutive empty rows.")
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
            
            # Polite sleep
            await asyncio.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"Error on {url}: {e}")
            continue

    return pd.DataFrame(results)

async def main():
    browser = None
    csv_file = "match_data.csv"
    
    # 1. Load existing data
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
        club_urls = [
             "https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
             "https://fbref.com/en/squads/206d90db/Barcelona-Stats",
             "https://fbref.com/en/squads/2a8183b3/Villarreal-Stats",
             "https://fbref.com/en/squads/db3b9613/Atletico-Madrid-Stats",
             "https://fbref.com/en/squads/a8661628/Espanyol-Stats",
             "https://fbref.com/en/squads/fc536746/Real-Betis-Stats",
             "https://fbref.com/en/squads/2b390eca/Athletic-Club-Stats",
             "https://fbref.com/en/squads/f25da7fb/Celta-Vigo-Stats",
             "https://fbref.com/en/squads/ad2be733/Sevilla-Stats",
             "https://fbref.com/en/squads/7848bd64/Getafe-Stats",
             "https://fbref.com/en/squads/6c8b07df/Elche-Stats",
             "https://fbref.com/en/squads/8d6fd021/Alaves-Stats",
             "https://fbref.com/en/squads/98e8af82/Rayo-Vallecano-Stats",
             "https://fbref.com/en/squads/2aa12281/Mallorca-Stats",
             "https://fbref.com/en/squads/9024a00a/Girona-Stats",
             "https://fbref.com/en/squads/ab358912/Oviedo-Stats",
             "https://fbref.com/en/squads/9800b6a1/Levante-Stats",
            #  Champions League clubs:
             "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
             "https://fbref.com/en/squads/054efa67/Bayern-Munich-Stats",
             "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
             "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats",
             "https://fbref.com/en/squads/922493f3/Atalanta-Stats",
             "https://fbref.com/en/squads/d609edc0/Internazionale-Stats",
             "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
             "https://fbref.com/en/squads/add600ae/Dortmund-Stats",
             "https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats",
             "https://fbref.com/en/squads/b2b47a98/Newcastle-United-Stats",
             "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
             "https://fbref.com/en/squads/13dc44fd/Sporting-CP-Stats",
             "https://fbref.com/en/squads/5725cc7b/Marseille-Stats",
             "https://fbref.com/en/squads/e0652b02/Juventus-Stats",
             "https://fbref.com/en/squads/ecd11ca2/Galatasaray-Stats",
             "https://fbref.com/en/squads/fd6114db/Monaco-Stats",
             "https://fbref.com/en/squads/c7a9f859/Bayer-Leverkusen-Stats",
             "https://fbref.com/en/squads/e334d850/PSV-Eindhoven-Stats",
             "https://fbref.com/en/squads/44b65410/Qarabag-Stats",
             "https://fbref.com/en/squads/d48ad4ff/Napoli-Stats",
             "https://fbref.com/en/squads/18050b20/FC-Copenhagen-Stats",
             "https://fbref.com/en/squads/a77c513e/Benfica-Stats",
             "https://fbref.com/en/squads/cb42669e/Pafos-FC-Stats",
             "https://fbref.com/en/squads/e14f61a5/Union-SG-Stats",
             "https://fbref.com/en/squads/2b390eca/Athletic-Club-Stats",
             "https://fbref.com/en/squads/2fdb4aef/Olympiacos-Stats",
             "https://fbref.com/en/squads/f0ac8ee6/Eintracht-Frankfurt-Stats",
             "https://fbref.com/en/squads/f1e6c5f1/Club-Brugge-Stats",
             "https://fbref.com/en/squads/d86248bd/BodoGlimt-Stats",
             "https://fbref.com/en/squads/111cbfb1/Slavia-Prague-Stats",
             "https://fbref.com/en/squads/19c3f8c4/Ajax-Stats",
             "https://fbref.com/en/squads/2a8183b3/Villarreal-Stats",
             "https://fbref.com/en/squads/768fb565/Qairat-Almaty-Stats",
        ]
        
        browser = await zd.start(headless=False)
        page = await browser.get("about:blank")
        
        # 2. Collect ALL potential URLs
        all_found_urls = []
        for url in club_urls:
            found = await scrape_all_club_matches_urls(url, page)
            all_found_urls.extend(found)
        
        unique_found_urls = list(set(all_found_urls))
        print(f"Total unique URLs found: {len(unique_found_urls)}")

        # 3. Filter
        urls_to_scrape = [u for u in unique_found_urls if u not in existing_urls]
        
        if not urls_to_scrape:
            print("No new matches found to scrape.")
        else:
            print(f"New matches to scrape: {len(urls_to_scrape)}")
            df_new = await scrape_all_club_matches(urls_to_scrape, page)
            
            if not df_new.empty:
                df_final = pd.concat([df_old, df_new], ignore_index=True)
                if 'match_url' in df_final.columns:
                    df_final = df_final.drop_duplicates(subset=['match_url'])
                
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