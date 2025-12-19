import asyncio
import zendriver as zd
import pandas as pd
import random

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
            return element # Return the element itself if needed
    except Exception:
        return None

async def get_page_content(url, page):
    print(f"Starting to scrape: {url}")
    
    # Retry configuration
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            # 1. Navigate
            await page.get(url)
            
            # 2. Smart Wait Strategy
            # We try to wait for "complete", but we don't let a timeout stop us.
            try:
                await page.wait_for_ready_state("complete", timeout=15)
            except Exception:
                # If "complete" times out (e.g. ads still loading), just print a warning 
                # and check if the content we actually need is there.
                print(f"   (Warning: 'complete' state timed out on attempt {attempt}, checking for content...)")

            # 3. The "Real" Check
            # We only care if the scorebox exists. If this passes, the page is good.
            await page.wait_for('#content > div.scorebox', timeout=10)
            
            # --- If we get here, the page is loaded enough to scrape! ---
            
            # Extract Data (using the safe_get helper you already have)
            home_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            away_team_name = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            
            home_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            away_team_score = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            
            home_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            away_team_xg = await safe_get(page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")

            # Managers
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
                print("   Retrying...")
                await asyncio.sleep(3) # Wait before retrying
            else:
                print(f"   Skipping match {url} after {max_retries} failed attempts.")
                return None

async def scrape_all_club_matches_urls(url, page):
    print("Starting to scrape all matches urls for the club:", url)
    urls = []

    # Navigate using the existing page
    await page.get(url)
    await page.wait_for('#content', timeout=20) 
    await asyncio.sleep(2)

    # Improved loop to avoid the Timeout error you saw earlier
    i = 1
    # Check up to 60 rows to be safe
    for i in range(1, 60):
        if i == 16: # Skip divider
            continue

        try:
            # Use a short timeout here. If element isn't found quickly, we are likely at the end.
            selector = f"#matchlogs_for > tbody > tr:nth-child({i}) > td.left.group_start > a"
            href = await page.select(selector, timeout=1)
            
            if not href:
                break # End of table
                
            if href.text != "Match Report":
                break # Reached rows that aren't matches
            else:
                match_url = f"https://fbref.com{href.get('href')}"
                print("match URL found:", match_url)
                urls.append(match_url)
        except Exception:
            # If selection fails, we assume we reached the end of the table
            break
            
    print("Total match URLs found:", len(urls))
    return urls

async def scrape_all_club_matches(urls, page):
    print("Starting to scrape all matches for the club:")
    
    results = []
    for i, url in enumerate(urls):
        print(f"\nProcessing {i+1}/{len(urls)}")
        try:
            # Pass the SAME page object to the function
            match_data = await get_page_content(url, page)
            if match_data:
                results.append(match_data)
            
            # Small random sleep to be polite and avoid blocks
            await asyncio.sleep(random.uniform(2, 5))
            
        except Exception as e:
            print(f"An error occurred while scraping match {url}: {e}")
            continue

    df = pd.DataFrame(results)
    print("DataFrame created successfully!")
    return df

async def main():
    browser = None
    try:
        club_urls = ["https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
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
                     ]
        browser = await zd.start(headless=False)
        
        # Open the first tab (or use the default one)
        # In zendriver, browser.get() returns the first tab if new_tab=False
        page = await browser.get("about:blank")
        
        # Pass this single 'page' object to all functions
        urls = []
        for url in club_urls:
            result = await scrape_all_club_matches_urls(url, page)
            urls.extend(result)

        if urls:
            df = await scrape_all_club_matches(urls, page)
            print(df)
            df.to_csv("match_data.csv", index=False)
            print(".csv file created successfully!")
        
    except Exception as e:
        print(f"An error occurred during execution: {e}")

    finally:
        if browser:
            await browser.stop() 
            print("Browser Stopped.")

if __name__ == "__main__":
    asyncio.run(main())