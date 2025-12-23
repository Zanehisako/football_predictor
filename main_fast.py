import asyncio
import zendriver as zd
import pandas as pd
import random
import os
import math
import time
from concurrent.futures import ProcessPoolExecutor
import zendriver.cdp.network as network # Import the network module


# --- CONFIGURATION ---
NUM_PROCESSES = 3  # Number of simultaneous browsers. 3 is safe. 5+ might freeze your PC.
HEADLESS = True    # Set False if you want to see the browsers (slower)

# --- 1. SHARED HELPER FUNCTIONS ---

async def safe_get(page, selector, attribute='text', timeout=1):
    """Safely gets an attribute from an element. Fast timeout to speed up misses."""
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
    """
    Scrapes a single match using the DETAILED selectors you provided.
    """
    # print(f"   ...Scraping {url.split('/')[-1]}")
    
    # Retry logic (2 attempts)
    for attempt in range(1, 3):
        try:
            await page.get(url)
            
            # Fast wait: Proceed as soon as 'complete' OR scorebox appears
            try:
                await page.wait_for_ready_state("complete", timeout=10)
            except:
                pass
            
            try:
                await page.wait_for('#content > div.scorebox', timeout=5)
            except:
                # If scorebox isn't found, page is likely broken or captcha'd
                return None

            # --- YOUR DETAILED EXTRACTION LOGIC ---
            
            # 1. Header Info
            home_team_name = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            away_team_name = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            home_team_score = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            away_team_score = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            home_team_xg = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            away_team_xg = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")

            # 2. Managers & Captains
            h_mgr = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(5)", 'text_all')
            a_mgr = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(5)", 'text_all')
            home_manager = h_mgr.split(":")[-1].strip() if h_mgr else None
            away_manager = a_mgr.split(":")[-1].strip() if a_mgr else None

            home_captain = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
            away_captain = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")

            # 3. Metadata
            match_time_full = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime", 'text_all')
            match_time = match_time_full.split()[0] if match_time_full else None
            attendance = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
            venue = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
            officials = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small", 'text_all')

            # 4. Standard Stats
            h_poss = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
            a_poss = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            h_pass_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
            a_pass_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            h_shot_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
            a_shot_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")
            
            h_save_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
            a_save_acc = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")

            h_cards = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div", 'child_count')
            a_cards = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div", 'child_count')

            # 5. Extra Stats (The ones you asked to add back)
            h_fouls = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(4)")
            a_fouls = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(6)")
            
            h_corners = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(7)")
            a_corners = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(9)")
            
            h_crosses = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(10)")
            a_crosses = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(12)")
            
            h_touches = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(13)")
            a_touches = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(15)")

            h_tackles = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(4)")
            a_tackles = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(6)")
            
            h_interceptions = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(7)")
            a_interceptions = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(9)")
            
            h_aerials = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(10)")
            a_aerials = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(12)")
            
            h_clearances = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(13)")
            a_clearances = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(15)")

            h_offsides = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(4)")
            a_offsides = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(6)")
            
            h_goalkicks = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(7)")
            a_goalkicks = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(9)")
            
            h_throwins = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(10)")
            a_throwins = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(12)")
            
            h_longballs = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(13)")
            a_longballs = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(15)")

            return {
                'match_url': url,
                'home_team': home_team_name, 'away_team': away_team_name,
                'home_score': home_team_score, 'away_score': away_team_score,
                'home_xg': home_team_xg, 'away_xg': away_team_xg,
                'home_manager': home_manager, 'away_manager': away_manager,
                'home_captain': home_captain, 'away_captain': away_captain,
                'match_time': match_time, 'attendance': attendance,
                'venue': venue, 'officials': officials,
                'home_possession': h_poss, 'away_possession': a_poss,
                'home_pass_acc': h_pass_acc, 'away_pass_acc': a_pass_acc,
                'home_shot_acc': h_shot_acc, 'away_shot_acc': a_shot_acc,
                'home_save_acc': h_save_acc, 'away_save_acc': a_save_acc,
                'home_cards': h_cards, 'away_cards': a_cards,
                'home_fouls': h_fouls, 'away_fouls': a_fouls,
                'home_corners': h_corners, 'away_corners': a_corners,
                'home_crosses': h_crosses, 'away_crosses': a_crosses,
                'home_touches': h_touches, 'away_touches': a_touches,
                'home_tackles': h_tackles, 'away_tackles': a_tackles,
                'home_interceptions': h_interceptions, 'away_interceptions': a_interceptions,
                'home_aerials': h_aerials, 'away_aerials': a_aerials,
                'home_clearances': h_clearances, 'away_clearances': a_clearances,
                'home_offsides': h_offsides, 'away_offsides': a_offsides,
                'home_goalkicks': h_goalkicks, 'away_goalkicks': a_goalkicks,
                'home_throwins': h_throwins, 'away_throwins': a_throwins,
                'home_longballs': h_longballs, 'away_longballs': a_longballs,
            }
        except Exception as e:
            # print(f"Retry {attempt} failed for {url}")
            await asyncio.sleep(1)
            
    return None

async def scrape_club_match_urls(page, club_url):
    """Scrapes match URLs from a club page. Tolerates gaps."""
    # print(f"   Getting list: {club_url.split('/')[-1]}")
    urls = []
    try:
        await page.get(club_url)
        try:
            await page.wait_for('#matchlogs_for', timeout=10)
        except:
            return []

        consecutive_failures = 0
        for i in range(1, 80):
            try:
                selector = f"#matchlogs_for > tbody > tr:nth-child({i}) > td[data-stat='match_report'] > a"
                link_el = await page.select(selector, timeout=0.1)
                
                if link_el and link_el.text == "Match Report":
                    urls.append(f"https://fbref.com{link_el.get('href')}")
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except:
                consecutive_failures += 1
            
            if consecutive_failures >= 5:
                break
    except:
        pass
    
    return urls

# --- 2. WORKER LOGIC ---

async def async_worker(club_urls, existing_urls):
    """Worker process with IMAGE BLOCKING enabled (Fixed function name)."""
    browser = None
    collected_data = []
    
    try:
        # 1. Start Browser
        browser = await zd.start(headless=HEADLESS)
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
        
        for club in club_urls:
            # 1. Get Matches
            match_urls = await scrape_club_match_urls(page, club)
            new_urls = [u for u in match_urls if u not in existing_urls]
            
            if not new_urls:
                continue
                
            print(f"   [Worker] Found {len(new_urls)} new matches for {club.split('/')[-1]}")
            
            # 2. Scrape Matches
            for url in new_urls:
                t0 = time.time()
                data = await get_page_content(url, page)
                duration = time.time() - t0
                
                if data:
                    collected_data.append(data)
                
                # Performance Warning
                if duration > 10:
                    print(f"      [WARNING] Slow network detected: {duration:.2f}s for one page")
                
                # Polite Sleep
                await asyncio.sleep(random.uniform(2, 3))
                
    except Exception as e:
        print(f"Worker Error: {e}")
    finally:
        if browser:
            await browser.stop()
            
    return collected_data

def run_worker_wrapper(args):
    return asyncio.run(async_worker(*args))

# --- 3. MAIN & BENCHMARKING ---

def main():
    start_time = time.time()
    
    # 1. Load Data
    csv_file = "match_data.csv"
    existing_urls = set()
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file)
            if 'match_url' in df.columns:
                existing_urls = set(df['match_url'].unique())
        except: pass

    # 2. Club List
    club_urls = [
         "https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
         "https://fbref.com/en/squads/206d90db/Barcelona-Stats",
         "https://fbref.com/en/squads/db3b9613/Atletico-Madrid-Stats",
         "https://fbref.com/en/squads/2a8183b3/Villarreal-Stats",
         "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
         "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
         "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
         "https://fbref.com/en/squads/054efa67/Bayern-Munich-Stats",
         "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats",
         # Add all your other URLs here...
    ]
    random.shuffle(club_urls)

    # 3. Batching
    chunk_size = math.ceil(len(club_urls) / NUM_PROCESSES)
    batches = []
    for i in range(0, len(club_urls), chunk_size):
        batches.append((club_urls[i:i + chunk_size], existing_urls))

    print(f"--- STARTING BENCHMARK ---")
    print(f"Processes: {NUM_PROCESSES}")
    print(f"Headless: {HEADLESS}")
    print(f"Total Clubs: {len(club_urls)}")
    print("--------------------------")

    # 4. Execution
    total_new_matches = 0
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for result in executor.map(run_worker_wrapper, batches):
            total_new_matches += len(result)
            
            # Intermediate Save (Optional but good safety)
            if result:
                df_temp = pd.DataFrame(result)
                df_temp.to_csv("match_data_temp.csv", mode='a', header=not os.path.exists("match_data_temp.csv"), index=False)

    # 5. Final Stats
    end_time = time.time()
    total_time = end_time - start_time
    
    # Save Final
    if os.path.exists("match_data_temp.csv"):
        df_new = pd.read_csv("match_data_temp.csv")
        if os.path.exists(csv_file):
            df_old = pd.read_csv(csv_file)
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new
            
        if 'match_url' in df_final.columns:
            df_final = df_final.drop_duplicates(subset=['match_url'])
        
        df_final.to_csv(csv_file, index=False)
        if os.path.exists("match_data_temp.csv"):
            os.remove("match_data_temp.csv")

    # 6. Benchmark Report
    print("\n--- BENCHMARK RESULTS ---")
    print(f"Total Time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"Total Matches Scraped: {total_new_matches}")
    if total_new_matches > 0:
        matches_per_min = (total_new_matches / total_time) * 60
        print(f"Speed: {matches_per_min:.2f} matches/minute")
        print(f"Avg Time per Match: {total_time/total_new_matches:.2f} seconds")
    else:
        print("No new matches were found (or scraping failed).")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped by user.")