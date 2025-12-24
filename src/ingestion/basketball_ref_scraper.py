"""
Basketball Reference Web Scraper - Production Version
Uses surgical string extraction to handle hidden tables in HTML comments
"""
import json
import logging
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List

from bs4 import BeautifulSoup
from curl_cffi import requests

from src.storage.storage_interface import get_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Changed back to INFO for production
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PlayerStats:
    """Data class representing a player's career statistics"""
    name: str
    player_id: str
    url: str
    
    # Career averages
    games_played: Optional[int] = None
    points_per_game: Optional[float] = None
    rebounds_per_game: Optional[float] = None
    assists_per_game: Optional[float] = None
    
    # Advanced metrics
    true_shooting_pct: Optional[float] = None
    player_efficiency_rating: Optional[float] = None
    box_plus_minus: Optional[float] = None
    win_shares: Optional[float] = None
    win_shares_per_48: Optional[float] = None
    value_over_replacement: Optional[float] = None
    
    # Accolades
    championships: Optional[int] = None
    mvp_awards: Optional[int] = None
    all_star_selections: Optional[int] = None
    all_nba_selections: Optional[int] = None
    
    # Metadata
    position: Optional[str] = None
    years_active: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values"""
        return {k: v for k, v in asdict(self).items() if v is not None}


class BasketballReferenceScraper:
    """
    Production scraper using surgical string extraction for hidden tables
    """
    
    BASE_URL = "https://www.basketball-reference.com"
    MIN_REQUEST_DELAY = 3.0
    
    def __init__(self, storage=None):
        """Initialize scraper with curl_cffi browser impersonation"""
        self.storage = storage or get_storage()
        self.session = requests.Session(impersonate="chrome120")
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Enforce minimum delay between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.MIN_REQUEST_DELAY:
            sleep_time = self.MIN_REQUEST_DELAY - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, max_retries: int = 3):
        """Make HTTP request with exponential backoff"""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                logger.info(f"Fetching: {url} (attempt {attempt + 1}/{max_retries})")
                
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 404:
                    logger.error(f"Player page not found: {url}")
                    return None
                elif response.status_code == 403:
                    wait_time = 2 ** attempt * 2
                    logger.warning(f"403 Forbidden. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response
                
            except Exception as e:
                logger.error(f"Request failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def _construct_player_url(self, player_id: str) -> str:
        """Construct player URL from ID"""
        first_letter = player_id[0].lower()
        return f"{self.BASE_URL}/players/{first_letter}/{player_id}.html"
    
    def _extract_table_surgically(self, html_text: str, data_stat_identifier: str) -> Optional[str]:
        """
        Surgical string extraction using data-stat attributes (more reliable than IDs)
        
        Strategy:
        1. Find a unique data-stat attribute that only appears in the target table
        2. Walk backwards to find the enclosing <table> tag
        3. Walk forwards to find the closing </table> tag
        4. Extract and return the substring
        
        Args:
            html_text: Raw HTML content
            data_stat_identifier: Unique data-stat value (e.g., 'pts_per_g' for per_game table)
        """
        try:
            # Search for the data-stat attribute
            search_pattern = f'data-stat="{data_stat_identifier}"'
            start_pos = html_text.find(search_pattern)
            
            if start_pos == -1:
                # Try single quotes
                search_pattern = f"data-stat='{data_stat_identifier}'"
                start_pos = html_text.find(search_pattern)
            
            if start_pos == -1:
                logger.debug(f"data-stat '{data_stat_identifier}' not found in HTML")
                return None
            
            logger.debug(f"Found data-stat='{data_stat_identifier}' at position {start_pos}")
            
            # Walk backwards to find the opening <table tag
            table_start = start_pos
            search_limit = max(0, start_pos - 50000)  # Don't search more than 50KB back
            
            while table_start > search_limit:
                if html_text[table_start:table_start+6].lower() == '<table':
                    break
                table_start -= 1
            
            if table_start == search_limit:
                logger.warning(f"Could not find <table tag before data-stat '{data_stat_identifier}'")
                return None
            
            logger.debug(f"Found <table at position {table_start}")
            
            # Walk forwards to find the closing </table> tag
            table_end = start_pos
            close_tag = '</table>'
            search_limit = min(len(html_text), start_pos + 100000)  # Don't search more than 100KB forward
            
            while table_end < search_limit:
                if html_text[table_end:table_end+len(close_tag)].lower() == close_tag:
                    table_end += len(close_tag)
                    break
                table_end += 1
            
            if table_end >= search_limit:
                logger.warning(f"Could not find </table> after data-stat '{data_stat_identifier}'")
                return None
            
            logger.debug(f"Found </table> at position {table_end}")
            
            # Extract the table HTML
            table_html = html_text[table_start:table_end]
            
            # Remove comment markers if present (Basketball Reference hides tables in comments)
            table_html = table_html.replace('<!--', '').replace('-->', '')
            
            logger.debug(f"Extracted table ({len(table_html)} chars)")
            return table_html
            
        except Exception as e:
            logger.error(f"Error in surgical extraction: {e}", exc_info=True)
            return None
    
    def _parse_table_cell(self, cell) -> Optional[str]:
        """Safely extract text from table cell"""
        if cell is None:
            return None
        text = cell.get_text(strip=True)
        return text if text else None
    
    def _parse_career_stats(self, html_text: str) -> Dict[str, Any]:
        """
        Extract career stats AND years active from the per-game table
        HYBRID: Uses Gemini's regex approach for Games (more reliable)
        """
        stats = {}
        
        try:
            table_html = self._extract_table_surgically(html_text, 'pts_per_g')
            
            if not table_html:
                logger.warning("Could not extract per_game table using pts_per_g")
                return stats
            
            soup = BeautifulSoup(table_html, 'html.parser')
            table = soup.find('table')
            
            if not table:
                logger.warning("Extracted HTML does not contain valid table")
                return stats
            
            # Find the Career row
            career_row = None
            
            def is_career_row(row):
                txt = row.get_text().strip()
                return 'Career' in txt or re.search(r'^\d+\s+Yrs', txt)
            
            tfoot = table.find('tfoot')
            if tfoot:
                for row in tfoot.find_all('tr'):
                    if is_career_row(row):
                        career_row = row
                        break
            
            if not career_row:
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        if is_career_row(row):
                            career_row = row
                            break
            
            if not career_row:
                logger.warning("Could not find Career row in per_game table")
                return stats
            
            logger.debug(f"Career row found! Content: {career_row.get_text()[:100]}")
            
            # Extract Years Active from first cell
            first_cell = career_row.find('th')
            if first_cell:
                years_text = first_cell.get_text().strip()
                years_match = re.search(r'(\d+)\s+Yrs', years_text)
                if years_match:
                    stats['years_active'] = years_match.group(1)
                elif 'Career' in years_text:
                    stats['years_active'] = 'Career'
            
            # GEMINI'S FIX: Extract Games using regex on full row text (more reliable)
            row_text_full = career_row.get_text().strip()
            games_match = re.search(r'^\d+\s+Yrs\s+(\d+)', row_text_full)
            
            if games_match:
                stats['games_played'] = games_match.group(1)
            else:
                # Fallback to cell lookup
                games_cell = career_row.find(['td', 'th'], {'data-stat': 'g'})
                stats['games_played'] = self._parse_table_cell(games_cell)
            
            # Extract other stats
            stats['points_per_game'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'pts_per_g'})
            )
            stats['rebounds_per_game'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'trb_per_g'})
            )
            stats['assists_per_game'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'ast_per_g'})
            )
            
            logger.info(f"Parsed career stats: Games={stats.get('games_played')}, PPG={stats.get('points_per_game')}, Years={stats.get('years_active')}")
            
        except Exception as e:
            logger.error(f"Error parsing career stats: {e}", exc_info=True)
        
        return stats
    
    def _parse_advanced_stats(self, html_text: str) -> Dict[str, Any]:
        """
        Extract advanced stats using data-stat attribute (more reliable)
        """
        stats = {}
        
        try:
            # Extract table using unique data-stat attribute
            # 'per' (Player Efficiency Rating) only appears in advanced stats table
            table_html = self._extract_table_surgically(html_text, 'per')
            
            if not table_html:
                logger.warning("Could not extract advanced table using 'per' data-stat")
                return stats
            
            # Parse the extracted table
            soup = BeautifulSoup(table_html, 'html.parser')
            table = soup.find('table')
            
            if not table:
                logger.warning("Extracted HTML does not contain valid advanced table")
                return stats
            
            # Find Career row - Basketball Reference shows it as "X Yrs" (e.g., "15 Yrs")
            career_row = None
            
            # Strategy 1: Check tfoot for row starting with number + "Yrs" or "Career"
            tfoot = table.find('tfoot')
            if tfoot:
                logger.debug("Searching advanced tfoot for Career row...")
                for row in tfoot.find_all('tr'):
                    row_text = row.get_text().strip()
                    
                    # Match "X Yrs" or "Career"
                    if (re.search(r'^\d+\s+Yrs', row_text) or 'Career' in row_text) and row.find('td'):
                        career_row = row
                        logger.debug(f"Found Career row in advanced tfoot: {row_text[:30]}")
                        break
            
            # Strategy 2: Check tbody
            if not career_row:
                tbody = table.find('tbody')
                if tbody:
                    logger.debug("Searching advanced tbody for Career row...")
                    for row in tbody.find_all('tr'):
                        row_text = row.get_text().strip()
                        
                        if (re.search(r'^\d+\s+Yrs', row_text) or 'Career' in row_text) and row.find('td'):
                            career_row = row
                            logger.debug(f"Found Career row in advanced tbody: {row_text[:30]}")
                            break
            
            # Strategy 3: Look for career class
            if not career_row:
                career_row = table.find('tr', class_=lambda x: x and 'career' in x.lower())
                if career_row and career_row.find('td'):
                    logger.debug("Found Career row in advanced table by class")
            
            if not career_row:
                logger.warning("Could not find Career row in advanced table")
                return stats
            
            logger.debug(f"Advanced Career row found! Content: {career_row.get_text()[:100]}")
            
            # Extract advanced stats
            stats['player_efficiency_rating'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'per'})
            )
            stats['true_shooting_pct'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'ts_pct'})
            )
            stats['box_plus_minus'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'bpm'})
            )
            stats['win_shares'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'ws'})
            )
            stats['win_shares_per_48'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'ws_per_48'})
            )
            stats['value_over_replacement'] = self._parse_table_cell(
                career_row.find('td', {'data-stat': 'vorp'})
            )
            
            logger.info(f"Parsed advanced stats: PER={stats.get('player_efficiency_rating')}, WS={stats.get('win_shares')}")
            
        except Exception as e:
            logger.error(f"Error parsing advanced stats: {e}", exc_info=True)
        
        return stats
    
    def _parse_bio_info(self, html_text: str) -> Dict[str, Any]:
        """
        Parse biographical information with FIXED position regex
        
        The position text comes AFTER the closing </strong> tag, not inside it.
        HTML structure: Position:\n</strong>\nShooting Guard and Small Forward\n&#9642;
        
        Critical fix: Handle HTML entity &#9642; (not Unicode ▪)
        """
        info = {}
        
        try:
            # FIXED POSITION REGEX - Handles both &#9642; (HTML entity) and ▪ (Unicode)
            # Pattern captures text AFTER </strong> up to the bullet point or next section
            pos_match = re.search(
                r'Position:\s*</strong>\s*([A-Za-z\s]+?)(?:\s*&#9642;|\s*▪|\s*<strong>|\s*Shoots)',
                html_text,
                re.DOTALL  # Critical: handles literal \n newlines
            )
            
            if pos_match:
                position = pos_match.group(1).strip()
                
                # Clean up whitespace and newlines
                position = ' '.join(position.split())
                
                # Handle "and" - keep only the first position mentioned
                # "Shooting Guard and Small Forward" -> "Shooting Guard"
                if ' and ' in position:
                    position = position.split(' and ')[0].strip()
                
                # Final validation: reasonable length and not empty
                if position and len(position) < 30:
                    info['position'] = position
                    logger.info(f"Found position: {position}")
                else:
                    logger.warning(f"Position extracted but invalid: '{position}'")
            else:
                logger.warning("Position regex did not match")
            
        except Exception as e:
            logger.error(f"Error parsing bio info: {e}", exc_info=True)
        
        return info
    
    def _count_accolades(self, html_text: str) -> Dict[str, int]:
        """
        Count accolades - CORRECTED with actual Basketball Reference formats
        Key finding: "All Star" appears with SPACE, not hyphen, and uses lowercase "x"
        """
        accolades = {
            'championships': 0,
            'mvp_awards': 0,
            'all_star_selections': 0,
            'all_nba_selections': 0
        }
        
        try:
            # Championships - Pattern: "6x NBA Champ"
            champ_match = re.search(
                r'(\d+)\s*[×x]\s*NBA\s+[Cc]hamp',
                html_text,
                re.IGNORECASE
            )
            if champ_match:
                champ_num = int(champ_match.group(1))
                if 1 <= champ_num <= 20:
                    accolades['championships'] = champ_num
                    logger.debug(f"Found {accolades['championships']} championships")
            
            # MVP Awards
            mvp_patterns = [
                r'(\d+)\s*[×x]\s*NBA\s+MVP',
                r'(\d+)\s*[×x]\s*MVP',
            ]
            
            for pattern in mvp_patterns:
                match = re.search(pattern, html_text, re.IGNORECASE)
                if match:
                    mvp_num = int(match.group(1))
                    if 1 <= mvp_num <= 10:
                        accolades['mvp_awards'] = mvp_num
                        logger.debug(f"Found {accolades['mvp_awards']} MVP awards")
                        break
            
            # ALL-STAR - CORRECTED FORMAT
            # Basketball Reference uses: "14x All Star" (space, not hyphen) and lowercase x
            allstar_patterns = [
                r'(\d+)\s*[×x]\s*All\s+Star',      # "14x All Star" (with space)
                r'(\d+)\s*[×x]\s*All-Star',        # "14x All-Star" (with hyphen)
                r'(\d+)\s+All-Star',                # "14 All-Star" (no x)
            ]
            
            for pattern in allstar_patterns:
                match = re.search(pattern, html_text, re.IGNORECASE)
                if match:
                    all_star_num = int(match.group(1))
                    if 1 <= all_star_num <= 30:
                        accolades['all_star_selections'] = all_star_num
                        logger.debug(f"Found {accolades['all_star_selections']} All-Star selections")
                        break
            
            # All-NBA Selections
            an_match = re.search(
                r'(\d+)\s*[×x]\s*All-NBA',
                html_text,
                re.IGNORECASE
            )
            if an_match:
                all_nba_num = int(an_match.group(1))
                if 1 <= all_nba_num <= 25:
                    accolades['all_nba_selections'] = all_nba_num
                    logger.debug(f"Found {accolades['all_nba_selections']} All-NBA selections")
            
            logger.info(f"Accolades: {accolades['championships']} championships, {accolades['mvp_awards']} MVPs, {accolades['all_star_selections']} All-Stars, {accolades['all_nba_selections']} All-NBA")
        
        except Exception as e:
            logger.error(f"Error counting accolades: {e}", exc_info=True)
        
        return accolades
    
    def scrape_player(self, player_id: str, player_name: str) -> Optional[PlayerStats]:
        """
        Scrape all stats for a single player
        """
        url = self._construct_player_url(player_id)
        logger.info(f"Scraping {player_name} ({player_id})")
        
        response = self._make_request(url)
        if not response:
            return None
        
        try:
            # Work with raw HTML text for surgical extraction
            html_text = response.text
            
            # Initialize player object
            player = PlayerStats(
                name=player_name,
                player_id=player_id,
                url=url
            )
            
            # Parse different sections
            career_stats = self._parse_career_stats(html_text)
            advanced_stats = self._parse_advanced_stats(html_text)
            bio_info = self._parse_bio_info(html_text)
            accolades = self._count_accolades(html_text)
            
            # Merge all data
            all_data = {**career_stats, **advanced_stats, **bio_info, **accolades}
            
            # Update player object with type conversion
            for key, value in all_data.items():
                if hasattr(player, key) and value:
                    try:
                        if key in ['games_played', 'championships', 'mvp_awards', 
                                   'all_star_selections', 'all_nba_selections']:
                            setattr(player, key, int(value))
                        elif key in ['points_per_game', 'rebounds_per_game', 'assists_per_game',
                                     'true_shooting_pct', 'player_efficiency_rating', 'box_plus_minus',
                                     'win_shares', 'win_shares_per_48', 'value_over_replacement']:
                            setattr(player, key, float(value))
                        else:
                            setattr(player, key, value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert {key}={value}")
            
            logger.info(f"Successfully scraped {player_name}")
            return player
            
        except Exception as e:
            logger.error(f"Error scraping {player_name}: {e}", exc_info=True)
            return None
    
    def save_player_data(self, player: PlayerStats):
        """Save player data to Bronze layer using storage interface"""
        try:
            data = player.to_dict()
            json_data = json.dumps(data, indent=2)
            key = f"bronze/players/{player.player_id}.json"
            self.storage.write(key, json_data.encode('utf-8'))
            logger.info(f"Saved {player.name} to {key}")
        except Exception as e:
            logger.error(f"Error saving {player.name}: {e}")
            raise


# GOAT Players dictionary
GOAT_PLAYERS = {
    # Active superstars
    'jamesle01': 'LeBron James',
    'curryst01': 'Stephen Curry',
    'duranke01': 'Kevin Durant',
    'antetgi01': 'Giannis Antetokounmpo',
    'jokicni01': 'Nikola Jokic',
    
    # Modern legends
    'bryanko01': 'Kobe Bryant',
    'duncati01': 'Tim Duncan',
    'onealsh01': "Shaquille O'Neal",
    'olajuha01': 'Hakeem Olajuwon',
    'wadedw01': 'Dwyane Wade',
    'leonaka01': 'Kawhi Leonard',
    'garneke01': 'Kevin Garnett',
    'nowitdi01': 'Dirk Nowitzki',
    'paulch01': 'Chris Paul',
    'iversal01': 'Allen Iverson',
    
    # Classic era
    'jordami01': 'Michael Jordan',
    'abdulka01': 'Kareem Abdul-Jabbar',
    'johnsom01': 'Magic Johnson',
    'birdla01': 'Larry Bird',
    'chambwi01': 'Wilt Chamberlain',
    'russebi01': 'Bill Russell',
    'ervinju01': 'Julius Erving',
    'roberde01': 'Oscar Robertson',
    'westje01': 'Jerry West',
    'bayloel01': 'Elgin Baylor',
    'malonmo01': 'Moses Malone',
    'robinda01': 'David Robinson',
    'malonka01': 'Karl Malone',
    'stockjo01': 'John Stockton',
    'barklch01': 'Charles Barkley',
}


def main():
    """Main execution function"""
    logger.info("Starting Basketball Reference scraper")
    logger.info(f"Scraping {len(GOAT_PLAYERS)} players")
    
    scraper = BasketballReferenceScraper()
    
    successful = 0
    failed = 0
    
    for player_id, player_name in GOAT_PLAYERS.items():
        try:
            player_stats = scraper.scrape_player(player_id, player_name)
            
            if player_stats:
                scraper.save_player_data(player_stats)
                successful += 1
            else:
                failed += 1
                logger.warning(f"Failed to scrape {player_name}")
                
        except Exception as e:
            failed += 1
            logger.error(f"Unexpected error scraping {player_name}: {e}")
    
    logger.info(f"Scraping complete: {successful} successful, {failed} failed")
    logger.info(f"Data saved to Bronze layer: bronze/players/")


if __name__ == "__main__":
    main()