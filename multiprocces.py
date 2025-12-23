import asyncio
import zendriver as zd
import pandas as pd
import random
import os
import math
from concurrent.futures import ProcessPoolExecutor

# --- 1. SHARED HELPER FUNCTIONS ---

async def safe_get(page,page, selector, attribute='text', timeout=2):
    """Safely gets an attribute from an element."""
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
    """Scrapes a single match page using your detailed selectors."""
    print(f"   [Scraping] {url}")
    max_retries = 2
    
    for attempt in range(1, max_retries + 1):
        try:
            await page.get(url)
            
            # Smart wait
            try:
                await page.wait_for_ready_state("complete", timeout=10)
            except:
                pass
            
            # Wait for main content to ensure page loaded
            try:
                await page.wait_for('#content > div.scorebox', timeout=8)
            except:
                print(f"      Scorebox not found for {url}")
                return None
            
            # --- YOUR DETAILED EXTRACTION LOGIC ---
            
            # Basic Info
            home_team_name = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(1) > strong > a")
            away_team_name = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(1) > strong > a")
            home_team_score = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score")
            away_team_score = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score")
            home_team_xg = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div.scores > div.score_xg")
            away_team_xg = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div.scores > div.score_xg")

            # Managers
            h_mgr_full = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(5)", 'text_all')
            a_mgr_full = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(5)", 'text_all')
            home_team_manager = h_mgr_full.split(":")[-1].strip() if h_mgr_full else None
            away_team_manager = a_mgr_full.split(":")[-1].strip() if a_mgr_full else None

            # Captains
            home_team_captain = await safe_get(page,page, "#content > div.scorebox > div:nth-child(1) > div:nth-child(6) > a")
            away_team_captain = await safe_get(page,page, "#content > div.scorebox > div:nth-child(2) > div:nth-child(6) > a")

            # Meta Data
            match_time_full = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(1) > span.localtime", 'text_all')
            match_time = match_time_full.split()[0] if match_time_full else None
            attendance = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(5) > small")
            venue = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(6) > small")
            officials = await safe_get(page,page, "#content > div.scorebox > div.scorebox_meta > div:nth-child(7) > small", 'text_all')

            # --- TEAM STATS TABLE ---
            home_team_possession = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_possession = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(3) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_pass_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_pass_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(5) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_shot_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_shot_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(7) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_save_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(1) > div > div:nth-child(1) > strong")
            away_team_save_accuracy = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(9) > td:nth-child(2) > div > div:nth-child(1) > strong")

            home_team_cards_number = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(1) > div > div > div", 'child_count')
            away_team_cards_number = await safe_get(page,page, "#team_stats > table > tbody > tr:nth-child(11) > td:nth-child(2) > div > div > div", 'child_count')

            # --- EXTRA STATS (Fouls, Corners, etc) ---
            home_team_fouls = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(4)")
            away_team_fouls = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(6)")

            home_team_corners = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(7)")
            away_team_corners = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(9)")

            home_team_crosses = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(10)")
            away_team_crosses = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(12)")

            home_team_touches = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(13)")
            away_team_touches = await safe_get(page,page, "#team_stats_extra > div:nth-child(1) > div:nth-child(15)")

            home_team_tackels = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(4)")
            away_team_tackels = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(6)")

            home_team_interceptions = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(7)")
            away_team_interceptions = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(9)")

            home_team_aerials = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(10)")
            away_team_aerials = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(12)")

            home_team_clearances = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(13)")
            away_team_clearances = await safe_get(page,page, "#team_stats_extra > div:nth-child(2) > div:nth-child(15)")

            home_team_offsides = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(4)")
            away_team_offsides = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(6)")

            home_team_goal_kicks = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(7)")
            away_team_goal_kicks = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(9)")

            home_team_throw_ins = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(10)")
            away_team_throw_ins = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(12)")

            home_team_long_balls = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(13)")
            away_team_long_balls = await safe_get(page,page, "#team_stats_extra > div:nth-child(3) > div:nth-child(15)")

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
                'home_team_pass_accuracy': home_team_pass_accuracy,
                'away_team_pass_accuracy': away_team_pass_accuracy,
                'home_team_shot_accuracy': home_team_shot_accuracy,
                'away_team_shot_accuracy': away_team_shot_accuracy,
                'home_team_save_accuracy': home_team_save_accuracy,
                'away_team_save_accuracy': away_team_save_accuracy,
                'home_team_cards_number': home_team_cards_number,
                'away_team_cards_number': away_team_cards_number,
                'home_team_fouls': home_team_fouls,
                'away_team_fouls': away_team_fouls,
                'home_team_corners': home_team_corners,
                'away_team_corners': away_team_corners,
                'home_team_crosses': home_team_crosses,
                'away_team_crosses': away_team_crosses,
                'home_team_touches': home_team_touches,
                'away_team_touches': away_team_touches,
                'home_team_tackels': home_team_tackels,
                'away_team_tackels': away_team_tackels,
                'home_team_interceptions': home_team_interceptions,
                'away_team_interceptions': away_team_interceptions,
                'home_team_aerials': home_team_aerials,
                'away_team_aerials': away_team_aerials,
                'home_team_clearances': home_team_clearances,
                'away_team_clearances': away_team_clearances,
                'home_team_offsides': home_team_offsides,
                'away_team_offsides': away_team_offsides,
                'home_team_goal_kicks': home_team_goal_kicks,
                'away_team_goal_kicks': away_team_goal_kicks,
                'home_team_throw_ins': home_team_throw_ins,
                'away_team_throw_ins': away_team_throw_ins,
                'home_team_long_balls': home_team_long_balls,
                'away_team_long_balls': away_team_long_balls
            }

        except Exception as e:
            await asyncio.sleep(2)
    return None

async def scrape_club_match_urls(page, club_url):
    """Scrapes all match URLs from a club page."""
    print(f" [Club] Getting matches for: {club_url}")
    urls = []
    try:
        await page.get(club_url)
        try:
            await page.wait_for('#matchlogs_for', timeout=15)
        except:
            print(f" ! Failed to load match table for {club_url}")
            return []

        consecutive_failures = 0
        for i in range(1, 80):
            try:
                # Selector for match report link
                selector = f"#matchlogs_for > tbody > tr:nth-child({i}) > td[data-stat='match_report'] > a"
                link_el = await page.select(selector, timeout=0.1)
                
                if link_el and link_el.text == "Match Report":
                    full_url = f"https://fbref.com{link_el.get('href')}"
                    urls.append(full_url)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except:
                consecutive_failures += 1
            
            if consecutive_failures >= 5:
                break
    except Exception as e:
        print(f" ! Error processing club page: {e}")
    
    return urls

# --- 2. WORKER PROCESS LOGIC ---

async def async_worker_main(club_urls, existing_urls):
    """
    The async entry point for a single process.
    """
    data_collected = []
    browser = None
    
    try:
        # HEADLESS=True is CRITICAL for multiprocessing to save RAM
        browser = await zd.start(headless=True) 
        page = await browser.get("about:blank")
        
        for club_url in club_urls:
            # 1. Get List of Matches
            match_urls = await scrape_club_match_urls(page, club_url)
            
            # 2. Filter New Matches
            new_matches = [u for u in match_urls if u not in existing_urls]
            
            if not new_matches:
                continue
                
            print(f" -> Found {len(new_matches)} new matches for club. Scraping...")
            
            # 3. Scrape Matches
            for url in new_matches:
                match_data = await get_page_content(url, page)
                if match_data:
                    data_collected.append(match_data)
                
                # Sleep to be polite (prevent IP ban)
                await asyncio.sleep(random.uniform(2, 4))
                
    except Exception as e:
        print(f"CRITICAL WORKER ERROR: {e}")
    finally:
        if browser:
            await browser.stop()
            
    return data_collected

def run_worker_process(args):
    """Wrapper to run async code in a synchronous process."""
    club_urls, existing_urls = args
    return asyncio.run(async_worker_main(club_urls, existing_urls))

# --- 3. MAIN CONTROLLER ---

def main():
    # --- A. SETUP ---
    csv_file = "match_data.csv"
    existing_urls = set()
    
    if os.path.exists(csv_file):
        try:
            df_old = pd.read_csv(csv_file)
            if 'match_url' in df_old.columns:
                existing_urls = set(df_old['match_url'].unique())
            print(f"Loaded {len(df_old)} existing matches.")
        except:
            pass

    # Your list of clubs
    all_club_urls = [
         # Spain
         "https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats",
         "https://fbref.com/en/squads/206d90db/Barcelona-Stats",
         "https://fbref.com/en/squads/db3b9613/Atletico-Madrid-Stats",
         "https://fbref.com/en/squads/2a8183b3/Villarreal-Stats",
         "https://fbref.com/en/squads/2b390eca/Athletic-Club-Stats",
         "https://fbref.com/en/squads/a8661628/Espanyol-Stats",
         "https://fbref.com/en/squads/fc536746/Real-Betis-Stats",
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
         
         # England
         "https://fbref.com/en/squads/18bb7c10/Arsenal-Stats",
         "https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats",
         "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats",
         "https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats",
         "https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats",
         "https://fbref.com/en/squads/19538871/Manchester-United-Stats",
         "https://fbref.com/en/squads/8602292d/Aston-Villa-Stats",
         "https://fbref.com/en/squads/47c64c55/Crystal-Palace-Stats",
         "https://fbref.com/en/squads/8ef52968/Sunderland-Stats",
         "https://fbref.com/en/squads/d3fd31cc/Everton-Stats",
         "https://fbref.com/en/squads/d07537b9/Brighton-and-Hove-Albion-Stats",
         "https://fbref.com/en/squads/4ba7cbea/Bournemouth-Stats",
         "https://fbref.com/en/squads/fd962109/Fulham-Stats",
         "https://fbref.com/en/squads/cd051869/Brentford-Stats",
         "https://fbref.com/en/squads/e4a775cb/Nottingham-Forest-Stats",
         "https://fbref.com/en/squads/5bfb9659/Leeds-United-Stats",
         "https://fbref.com/en/squads/7c21e445/West-Ham-United-Stats",
         "https://fbref.com/en/squads/943e8050/Burnley-Stats",
         "https://fbref.com/en/squads/8cec06e1/Wolverhampton-Wanderers-Stats",

         # Germany
         "https://fbref.com/en/squads/054efa67/Bayern-Munich-Stats",
         "https://fbref.com/en/squads/add600ae/Dortmund-Stats",
         "https://fbref.com/en/squads/c7a9f859/Bayer-Leverkusen-Stats",
         "https://fbref.com/en/squads/acbb6a5b/RB-Leipzig-Stats",
         "https://fbref.com/en/squads/033ea6b8/Hoffenheim-Stats",
         "https://fbref.com/en/squads/598bc722/Stuttgart-Stats",
         "https://fbref.com/en/squads/7a41008f/Union-Berlin-Stats",
         "https://fbref.com/en/squads/a486e511/Freiburg-Stats",
         "https://fbref.com/en/squads/bc357bf7/Koln-Stats",
         "https://fbref.com/en/squads/32f3ee20/Monchengladbach-Stats",
         "https://fbref.com/en/squads/62add3bf/Werder-Bremen-Stats",
         "https://fbref.com/en/squads/4eaa11d7/Wolfsburg-Stats",
         "https://fbref.com/en/squads/26790c6a/Hamburger-SV-Stats",
         "https://fbref.com/en/squads/0cdc4311/Augsburg-Stats",
         "https://fbref.com/en/squads/54864664/St-Pauli-Stats",
         "https://fbref.com/en/squads/18d9d2a7/Heidenheim-Stats",
         "https://fbref.com/en/squads/a224b06a/Mainz-05-Stats",

         # France
         "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats",
         "https://fbref.com/en/squads/5725cc7b/Marseille-Stats",
         "https://fbref.com/en/squads/d53c0b06/Lyon-Stats",
         "https://fbref.com/en/squads/fd6114db/Monaco-Stats",
         "https://fbref.com/en/squads/fd4e0f7d/Lens-Stats",
         "https://fbref.com/en/squads/cb188c0c/Lille-Stats",
         "https://fbref.com/en/squads/b3072e00/Rennes-Stats",
         "https://fbref.com/en/squads/3f8c4b5f/Toulouse-Stats",
         "https://fbref.com/en/squads/c0d3eab4/Strasbourg-Stats",
         "https://fbref.com/en/squads/69236f98/Angers-Stats",
         "https://fbref.com/en/squads/fb08dbb3/Brest-Stats",
         "https://fbref.com/en/squads/d2c87802/Lorient-Stats",
         "https://fbref.com/en/squads/132ebc33/Nice-Stats",
         "https://fbref.com/en/squads/056a5a75/Paris-FC-Stats",
         "https://fbref.com/en/squads/5c2737db/Le-Havre-Stats",
         "https://fbref.com/en/squads/5ae09109/Auxerre-Stats",
         "https://fbref.com/en/squads/d7a486cd/Nantes-Stats",
         "https://fbref.com/en/squads/f83960ae/Metz-Stats",

         # Italy
         "https://fbref.com/en/squads/dc56fe14/Milan-Stats",
         "https://fbref.com/en/squads/d609edc0/Internazionale-Stats",
         "https://fbref.com/en/squads/e0652b02/Juventus-Stats",
         "https://fbref.com/en/squads/d48ad4ff/Napoli-Stats",
         "https://fbref.com/en/squads/922493f3/Atalanta-Stats",
         "https://fbref.com/en/squads/cf74a709/Roma-Stats",
         "https://fbref.com/en/squads/1d8099f8/Bologna-Stats",
         "https://fbref.com/en/squads/28c9c3cd/Como-Stats",
         "https://fbref.com/en/squads/7213da33/Lazio-Stats",
         "https://fbref.com/en/squads/e2befd26/Sassuolo-Stats",
         "https://fbref.com/en/squads/04eea015/Udinese-Stats",
         "https://fbref.com/en/squads/9aad3a77/Cremonese-Stats",
         "https://fbref.com/en/squads/105360fe/Torino-Stats",
         "https://fbref.com/en/squads/ffcbe334/Lecce-Stats",
         "https://fbref.com/en/squads/c4260e09/Cagliari-Stats",
         "https://fbref.com/en/squads/658bf2de/Genoa-Stats",
         "https://fbref.com/en/squads/eab4234c/Parma-Stats",
         "https://fbref.com/en/squads/0e72edf2/Hellas-Verona-Stats",
         "https://fbref.com/en/squads/4cceedfc/Pisa-Stats",
         "https://fbref.com/en/squads/421387cf/Fiorentina-Stats",

         # Others
         "https://fbref.com/en/squads/13dc44fd/Sporting-CP-Stats",
         "https://fbref.com/en/squads/ecd11ca2/Galatasaray-Stats",
         "https://fbref.com/en/squads/e334d850/PSV-Eindhoven-Stats",
         "https://fbref.com/en/squads/44b65410/Qarabag-Stats",
         "https://fbref.com/en/squads/18050b20/FC-Copenhagen-Stats",
         "https://fbref.com/en/squads/a77c513e/Benfica-Stats",
         "https://fbref.com/en/squads/cb42669e/Pafos-FC-Stats",
         "https://fbref.com/en/squads/e14f61a5/Union-SG-Stats",
         "https://fbref.com/en/squads/2fdb4aef/Olympiacos-Stats",
         "https://fbref.com/en/squads/f0ac8ee6/Eintracht-Frankfurt-Stats",
         "https://fbref.com/en/squads/f1e6c5f1/Club-Brugge-Stats",
         "https://fbref.com/en/squads/d86248bd/BodoGlimt-Stats",
         "https://fbref.com/en/squads/111cbfb1/Slavia-Prague-Stats",
         "https://fbref.com/en/squads/19c3f8c4/Ajax-Stats",
         "https://fbref.com/en/squads/768fb565/Qairat-Almaty-Stats",
    ]
    
    # Randomize to distribute load
    random.shuffle(all_club_urls)

    # --- B. PREPARE BATCHES ---
    NUM_PROCESSES = 4 
    
    chunk_size = math.ceil(len(all_club_urls) / NUM_PROCESSES)
    batches = []
    for i in range(0, len(all_club_urls), chunk_size):
        batch = all_club_urls[i:i + chunk_size]
        batches.append((batch, existing_urls))
        
    print(f"Starting {len(batches)} processes to scrape {len(all_club_urls)} clubs...")

    # --- C. RUN MULTIPROCESSING ---
    results_flat = []
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for result in executor.map(run_worker_process, batches):
            results_flat.extend(result)

    # --- D. SAVE RESULTS ---
    print(f"\nScraping complete. Collected {len(results_flat)} new match records.")
    
    if results_flat:
        df_new = pd.DataFrame(results_flat)
        
        # Load fresh CSV again in case it changed
        if os.path.exists(csv_file):
            try:
                df_old = pd.read_csv(csv_file)
                # Ensure columns match, adding missing ones if necessary
                df_final = pd.concat([df_old, df_new], ignore_index=True)
            except:
                 df_final = df_new
        else:
            df_final = df_new
            
        # Drop duplicates based on URL
        if 'match_url' in df_final.columns:
            df_final = df_final.drop_duplicates(subset=['match_url'])
            
        df_final.to_csv(csv_file, index=False)
        print(f"Data saved successfully to {csv_file}")
    else:
        print("No new data to save.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Scraping stopped by user.")