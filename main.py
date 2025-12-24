import asyncio
import zendriver as zd
import pandas as pd
import random
import os
import zendriver.cdp.network as network # Import the network module
import numpy as np

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

    # Find all strong elements with "Manager" text  
    manager_strongs = await page.select_all("strong")  
    manager_elements = [elem for elem in manager_strongs  if "Manager" in elem.text]  
    
    for i, manager in enumerate(manager_elements):  
        match i:
            case 0:
                manager_info= manager.parent.text_all.split()
                home_team_manager= manager_info[-2] + " " + manager_info[-1]
                print(f"manager:{home_team_manager}")
            case 1:
                manager_info= manager.parent.text_all.split()
                away_team_manager = manager_info[-2] + " " + manager_info[-1]
                print(f"manager:{away_team_manager}")

    captins= await page.find_all("Captain")
    for i,captin in enumerate(captins):
        match i:
            case 0:
                # print(f"home_captin{captin}")
                # Get the parent element  
                parent = captin.parent  
                home_team_capatin= await parent.query_selector("a")  
                # if home_team_capatin:
                #      print(f"home team cap:{home_team_capatin.text}")
            case 1:
                # print(f"away_captin{captin}")
                # Get the parent element  
                parent = captin.parent  
                away_team_capatin= await parent.query_selector("a")  
                # if away_team_capatin:
                #     print(f"away team cap:{away_team_capatin.text}")

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

    #Just to find the fucking passing bruv!
    try:
        passing_accuracy= await page.find("Passing Accuracy")
        parent = passing_accuracy.parent  
        grandpa = parent.parent
        # Get all children and filter to only element nodes (skip text nodes)  
        grandpa_children = [child for child in grandpa.children ]  
        # print("element children",grandpa_children)
        parent_passing_index= grandpa_children.index(parent)  
        # print(f"passing index:{parent_passing_index}")
        passing_parent = grandpa_children[parent_passing_index+1]
        parent_children= [child for child in passing_parent.children ]  

        home_passing = parent_children[0].children[0].children[0].text.split()
        home_passing_onTarget= home_passing[0]
        home_total_passing = home_passing[2]
        away_passing = parent_children[1].children[0].children[0].text_all.split()
        away_passing_onTarget= away_passing[2]
        away_total_passing = away_passing[-1]
        print(f"home passing on Target:{home_passing_onTarget}")
        print(f"home all passing:{home_total_passing}")
        print(f"away passing on Target:{away_passing_onTarget}")
        print(f"away passing passing:{away_total_passing}")
    except:
        passing_accuracy = None

    #Just to find the fucking shots bruv!
    home_team_all_shots= await page.find("Shots on Target")
    parent = home_team_all_shots.parent  
    grandpa = parent.parent
    # Get all children and filter to only element nodes (skip text nodes)  
    grandpa_children = [child for child in grandpa.children ]  
    # print("element children",grandpa_children)
    parent_shots_index= grandpa_children.index(parent)  
    # print(f"shots index:{parent_shots_index}")
    shots_parent = grandpa_children[parent_shots_index+1]
    parent_children= [child for child in shots_parent.children ]  

    home_shots = parent_children[0].children[0].children[0].text.split()
    home_shots_onTarget= home_shots[0]
    home_total_shots = home_shots[2]
    away_shots = parent_children[1].children[0].children[0].text_all.split()
    away_shots_onTarget= away_shots[2]
    away_total_shots = away_shots[-1]
    print(f"home shots on Target:{home_shots_onTarget}")
    print(f"home all shots:{home_total_shots}")
    print(f"away shots on Target:{away_shots_onTarget}")
    print(f"away all shots:{away_total_shots}")

    #Just to find the fucking saves bruv!
    home_team_all_saves= await page.find("Saves")
    parent = home_team_all_saves.parent  
    grandpa = parent.parent
    # Get all children and filter to only element nodes (skip text nodes)  
    grandpa_children = [child for child in grandpa.children ]  
    # print("element children",grandpa_children)
    parent_saves_index= grandpa_children.index(parent)  
    # print(f"saves index:{parent_saves_index}")
    saves_parent = grandpa_children[parent_saves_index+1]
    parent_children= [child for child in saves_parent.children ]  

    home_saves = parent_children[0].children[0].children[0].text.split()
    home_saves_onTarget= home_saves[0]
    home_total_saves = home_saves[2]
    away_saves = parent_children[1].children[0].children[0].text_all.split()
    away_saves_onTarget= away_saves[2]
    away_total_saves = away_saves[-1]
    print(f"home saves on Target:{home_saves_onTarget}")
    print(f"home all saves:{home_total_saves}")
    print(f"away saves on Target:{away_saves_onTarget}")
    print(f"away all saves:{away_total_saves}")


    #Just to find the fucking cards bruv!
    home_team_all_cards= await page.find("Cards")
    parent = home_team_all_cards.parent  
    grandpa = parent.parent
    # Get all children and filter to only element nodes (skip text nodes)  
    grandpa_children = [child for child in grandpa.children ]  
    parent_cards_index= grandpa_children.index(parent)  
    cards_parent = grandpa_children[parent_cards_index+1]
    parent_children= [child for child in cards_parent.children ]  

    home_cards = parent_children[0].children[0].children[0].children[0].child_node_count 
    away_cards = parent_children[0].children[0].children[0].children[0].child_node_count 
    print(f"home cards:{home_cards}")
    print(f"away cards:{away_cards}")

    #Just to find the fucking fouls bruv!
    fouls= await page.find("Fouls",best_match=True)
    parent = fouls.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    fouls_index= parent_children.index(fouls)  
    # print(f"fouls index:{parent_fouls_index}")
    home_fouls= parent_children[fouls_index-1]
    away_fouls= parent_children[fouls_index+1]

    print(f"home fouls:{home_fouls.text}")
    print(f"away fouls:{away_fouls.text}")

    #Just to find the fucking corners bruv!
    corners= await page.find("corners",best_match=True)
    parent = corners.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    corners_index= parent_children.index(corners)  
    # print(f"corners index:{parent_corners_index}")
    home_corners= parent_children[corners_index-1]
    away_corners= parent_children[corners_index+1]

    print(f"home corners:{home_corners.text}")
    print(f"away corners:{away_corners.text}")

    #Just to find the fucking crosses bruv!
    crosses= await page.find("crosses",best_match=True)
    parent = crosses.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    crosses_index= parent_children.index(crosses)  
    # print(f"crosses index:{parent_crosses_index}")
    home_crosses= parent_children[crosses_index-1]
    away_crosses= parent_children[crosses_index+1]

    print(f"home crosses:{home_crosses.text}")
    print(f"away crosses:{away_crosses.text}")

    #Just to find the fucking touches bruv!
    touches= await page.find("touches",best_match=True)
    parent = touches.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    touches_index= parent_children.index(touches)  
    # print(f"touches index:{parent_touches_index}")
    home_touches= parent_children[touches_index-1]
    away_touches= parent_children[touches_index+1]

    print(f"home touches:{home_touches.text}")
    print(f"away touches:{away_touches.text}")

    #Just to find the fucking tackles bruv!
    tackles= await page.find("Tackles",best_match=True)
    parent = tackles.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    tackles_index= parent_children.index(tackles)  
    # print(f"tackles index:{parent_tackles_index}")
    home_tackels= parent_children[tackles_index-1]
    away_tackels= parent_children[tackles_index+1]

    print(f"home tackles:{home_tackels.text}")
    print(f"away tackles:{away_tackels.text}")

    #Just to find the fucking interceptions bruv!
    interceptions= await page.find("Interceptions",best_match=True)
    parent = interceptions.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    interceptions_index= parent_children.index(interceptions)  
    # print(f"interceptions index:{parent_interceptions_index}")
    home_interceptions= parent_children[interceptions_index-1]
    away_interceptions= parent_children[interceptions_index+1]

    print(f"home interceptions:{home_interceptions.text}")
    print(f"away interceptions:{away_interceptions.text}")


    #Just to find the fucking aerials bruv!
    aerials= await page.find("Aerials Won",best_match=True)
    parent = aerials.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    aerials_index= parent_children.index(aerials)  
    # print(f"aerials index:{parent_aerials_index}")
    home_aerials= parent_children[aerials_index-1]
    away_aerials= parent_children[aerials_index+1]

    print(f"home aerials:{home_aerials.text}")
    print(f"away aerials:{away_aerials.text}")

    #Just to find the fucking clearances bruv!
    clearances= await page.find("Clearances",best_match=True)
    parent = clearances.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    clearances_index= parent_children.index(clearances)  
    # print(f"clearances index:{parent_clearances_index}")
    home_clearances= parent_children[clearances_index-1]
    away_clearances= parent_children[clearances_index+1]

    print(f"home clearances:{home_clearances.text}")
    print(f"away clearances:{away_clearances.text}")

    #Just to find the fucking offsides bruv!
    offsides= await page.find("Offsides",best_match=True)
    parent = offsides.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    offsides_index= parent_children.index(offsides)  
    # print(f"offsides index:{parent_offsides_index}")
    home_offsides= parent_children[offsides_index-1]
    away_offsides= parent_children[offsides_index+1]

    print(f"home offsides:{home_offsides.text}")
    print(f"away offsides:{away_offsides.text}")

    #Just to find the fucking goal kicks bruv!
    goal_kicks= await page.find("Goal Kicks",best_match=True)
    parent = goal_kicks.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    goal_kicks_index= parent_children.index(goal_kicks)  
    # print(f"goal kicks index:{parent_goal_kicks_index}")
    home_goal_kicks= parent_children[goal_kicks_index-1]
    away_goal_kicks= parent_children[goal_kicks_index+1]

    print(f"home goal kicks:{home_goal_kicks.text}")
    print(f"away goal kicks:{away_goal_kicks.text}")

    #Just to find the fucking throw ins bruv!
    throw_ins= await page.find("Throw Ins",best_match=True)
    parent = throw_ins.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    throw_ins_index= parent_children.index(throw_ins)  
    # print(f"throw ins index:{parent_throw_ins_index}")
    home_throw_ins= parent_children[throw_ins_index-1]
    away_throw_ins= parent_children[throw_ins_index+1]

    print(f"home throw ins:{home_throw_ins.text}")
    print(f"away throw ins:{away_throw_ins.text}")

    #Just to find the fucking long balls bruv!
    long_balls= await page.find("Long Balls",best_match=True)
    parent = long_balls.parent  
    # Get all children and filter to only element nodes (skip text nodes)  
    parent_children = [child for child in parent.children ]  
    # print("element children",grandpa_children)
    long_balls_index= parent_children.index(long_balls)  
    # print(f"long balls index:{parent_long_balls_index}")
    home_long_balls= parent_children[long_balls_index-1]
    away_long_balls= parent_children[long_balls_index+1]

    print(f"home long balls:{home_long_balls.text}")
    print(f"away long balls:{away_long_balls.text}")

    print("finished scrapping the page:", url)
    return {
        'match_url':url,
        'home_team_name': home_team_name.text,
        'away_team_name': away_team_name.text,
        'home_team_score': home_team_score.text,
        'away_team_score': away_team_score.text,
        'home_team_xg': home_team_xg.text if home_team_xg != None else ((float(home_shots_onTarget)*0.30)+((float(home_total_shots)-float(home_shots_onTarget))*0.05)) ,
        'away_team_xg': away_team_xg.text if away_team_xg != None else ((float(away_shots_onTarget)*0.30)+((float(away_total_shots)-float(away_shots_onTarget))*0.05)),
        'xg_is_estimated':False if home_team_xg else True,
        'home_team_manager': home_team_manager,
        'away_team_manager': away_team_manager,
        'home_team_captain': home_team_capatin.text,
        'away_team_captain': away_team_capatin.text,
        'match_time': match_time.text_all.split()[0],
        # 'attendance': attendance.text,
        # 'venue': venue.text,
        'officials': officials.text_all,
        'home_team_possession': home_team_possession.text,
        'away_team_possession': away_team_possession.text,
        'home_passing_onTarget': home_passing_onTarget if passing_accuracy!= None else np.nan,
        'home_total_passing': home_total_passing if passing_accuracy!= None else np.nan,
        'away_passing_onTarget': away_passing_onTarget if passing_accuracy!= None else np.nan,
        'away_total_passing': away_total_passing if passing_accuracy != None else np.nan,
        'home_shots_onTarget': home_shots_onTarget,
        'home_total_shots': home_total_shots,
        'away_shots_onTarget': away_shots_onTarget,
        'away_total_shots': away_total_shots,
        'home_saves_onTarget': home_saves_onTarget,
        'home_total_saves': home_total_saves,
        'away_saves_onTarget': away_saves_onTarget,
        'away_total_saves': away_total_saves,
        'home_team_cards_number': home_cards,
        'away_team_cards_number': away_cards,
        'home_fouls': home_fouls.text,
        'away_fouls': away_fouls.text,
        'home_corners': home_corners.text if home_corners != None else np.nan,
        'away_corners': away_corners.text if away_corners != None else np.nan,
        'home_crosses': home_crosses.text if home_crosses != None else np.nan,
        'away_crosses': away_crosses.text if away_crosses != None else np.nan,
        'home_touches': home_touches.text if home_touches != None else np.nan,
        'away_touches': away_touches.text if away_touches != None else np.nan,
        'home_tackels': home_tackels.text if home_tackels != None else np.nan,
        'away_tackels': away_tackels.text if away_tackels != None else np.nan,
        'home_interceptions': home_interceptions.text if home_interceptions != None else np.nan,
        'away_interceptions': away_interceptions.text if away_interceptions != None else np.nan,
        'home_aerials': home_aerials.text if home_aerials != None else np.nan,
        'away_aerials': away_aerials.text if away_aerials != None else np.nan,
        'home_clearances': home_clearances.text if home_clearances != None else np.nan,
        'away_clearances': away_clearances.text if away_clearances != None else np.nan,
        'home_offsides': home_offsides.text if home_offsides != None else np.nan,
        'away_offsides': away_offsides.text if away_offsides != None else np.nan,
        'home_goal_kicks': home_goal_kicks.text if home_goal_kicks != None else np.nan,
        'away_goal_kicks': away_goal_kicks.text if away_goal_kicks != None else np.nan,
        'home_throw_ins': home_throw_ins.text if home_throw_ins != None else np.nan,
        'away_throw_ins': away_throw_ins.text if away_throw_ins != None else np.nan,
        'home_long_balls': home_long_balls.text if home_long_balls != None else np.nan,
        'away_long_balls': away_long_balls.text if away_long_balls != None else np.nan,
    }


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
            # await asyncio.sleep(random.uniform(2, 4))
            
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