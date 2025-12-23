"""
Unit tests for Basketball Reference scraper

Tests cover:
- URL construction
- Rate limiting
- Data parsing
- Error handling
- Storage integration
"""

import json
import time
from unittest.mock import Mock, patch, MagicMock
import pytest
from bs4 import BeautifulSoup

from src.ingestion.basketball_ref_scraper import (
    BasketballReferenceScraper,
    PlayerStats,
    GOAT_PLAYERS
)


class TestPlayerStats:
    """Tests for PlayerStats data class"""
    
    def test_player_stats_creation(self):
        """Test creating a PlayerStats object"""
        player = PlayerStats(
            name="Test Player",
            player_id="testpl01",
            url="https://example.com",
            points_per_game=25.5,
            rebounds_per_game=10.2
        )
        
        assert player.name == "Test Player"
        assert player.player_id == "testpl01"
        assert player.points_per_game == 25.5
        assert player.rebounds_per_game == 10.2
    
    def test_to_dict_excludes_none(self):
        """Test that to_dict() excludes None values"""
        player = PlayerStats(
            name="Test Player",
            player_id="testpl01",
            url="https://example.com",
            points_per_game=25.5,
            rebounds_per_game=None  # Should be excluded
        )
        
        data = player.to_dict()
        
        assert 'points_per_game' in data
        assert 'rebounds_per_game' not in data
        assert data['name'] == "Test Player"


class TestBasketballReferenceScraper:
    """Tests for BasketballReferenceScraper"""
    
    @pytest.fixture
    def mock_storage(self):
        """Create a mock storage interface"""
        storage = Mock()
        storage.write = Mock()
        storage.read = Mock()
        return storage
    
    @pytest.fixture
    def scraper(self, mock_storage):
        """Create scraper with mock storage"""
        return BasketballReferenceScraper(storage=mock_storage)
    
    def test_construct_player_url(self, scraper):
        """Test URL construction from player ID"""
        # Test with various player IDs
        test_cases = [
            ("jamesle01", "https://www.basketball-reference.com/players/j/jamesle01.html"),
            ("jordami01", "https://www.basketball-reference.com/players/j/jordami01.html"),
            ("curryst01", "https://www.basketball-reference.com/players/c/curryst01.html"),
        ]
        
        for player_id, expected_url in test_cases:
            url = scraper._construct_player_url(player_id)
            assert url == expected_url
    
    def test_rate_limiting(self, scraper):
        """Test that rate limiting enforces minimum delay"""
        scraper.last_request_time = time.time()
        
        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start
        
        # Should have waited approximately MIN_REQUEST_DELAY seconds
        assert elapsed >= scraper.MIN_REQUEST_DELAY - 0.1
    
    def test_rate_limiting_no_delay_when_enough_time_passed(self, scraper):
        """Test that rate limiting doesn't delay when enough time has passed"""
        # Set last request time to 10 seconds ago
        scraper.last_request_time = time.time() - 10
        
        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start
        
        # Should not have waited
        assert elapsed < 0.1
    
    @patch('src.ingestion.basketball_ref_scraper.requests.Session.get')
    def test_make_request_success(self, mock_get, scraper):
        """Test successful HTTP request"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        response = scraper._make_request("https://example.com")
        
        assert response is not None
        assert response.status_code == 200
        mock_get.assert_called_once()
    
    @patch('src.ingestion.basketball_ref_scraper.requests.Session.get')
    def test_make_request_404_returns_none(self, mock_get, scraper):
        """Test that 404 errors return None"""
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status = Mock(
            side_effect=Exception("404 Not Found")
        )
        mock_get.return_value = mock_response
        
        response = scraper._make_request("https://example.com")
        
        assert response is None
    
    @patch('src.ingestion.basketball_ref_scraper.requests.Session.get')
    @patch('time.sleep')
    def test_make_request_retries_on_failure(self, mock_sleep, mock_get, scraper):
        """Test that failed requests are retried with backoff"""
        # Mock failures then success
        mock_get.side_effect = [
            Exception("Connection error"),
            Exception("Connection error"),
            Mock(status_code=200, raise_for_status=Mock())
        ]
        
        response = scraper._make_request("https://example.com", max_retries=3)
        
        assert response is not None
        assert mock_get.call_count == 3
        # Should have slept twice (after first two failures)
        assert mock_sleep.call_count == 2
    
    def test_parse_table_cell(self, scraper):
        """Test parsing table cell text"""
        # Test with valid cell
        html = '<td>25.5</td>'
        soup = BeautifulSoup(html, 'html.parser')
        cell = soup.find('td')
        
        result = scraper._parse_table_cell(cell)
        assert result == "25.5"
        
        # Test with None
        result = scraper._parse_table_cell(None)
        assert result is None
        
        # Test with empty cell
        html = '<td></td>'
        soup = BeautifulSoup(html, 'html.parser')
        cell = soup.find('td')
        
        result = scraper._parse_table_cell(cell)
        assert result is None
    
    def test_parse_career_stats(self, scraper):
        """Test parsing career stats from HTML"""
        # Create mock HTML for Per Game table
        html = '''
        <table id="per_game">
            <tfoot>
                <tr>
                    <th data-stat="season">Career</th>
                    <td data-stat="g">1000</td>
                    <td data-stat="pts_per_g">27.1</td>
                    <td data-stat="trb_per_g">7.4</td>
                    <td data-stat="ast_per_g">7.4</td>
                </tr>
            </tfoot>
        </table>
        '''
        
        soup = BeautifulSoup(html, 'html.parser')
        stats = scraper._parse_career_stats(soup)
        
        assert stats['games_played'] == "1000"
        assert stats['points_per_game'] == "27.1"
        assert stats['rebounds_per_game'] == "7.4"
        assert stats['assists_per_game'] == "7.4"
    
    def test_parse_advanced_stats(self, scraper):
        """Test parsing advanced stats from HTML"""
        html = '''
        <table id="advanced">
            <tfoot>
                <tr>
                    <th data-stat="season">Career</th>
                    <td data-stat="per">27.5</td>
                    <td data-stat="ts_pct">.585</td>
                    <td data-stat="bpm">8.9</td>
                    <td data-stat="ws">214.0</td>
                    <td data-stat="ws_per_48">.250</td>
                    <td data-stat="vorp">145.6</td>
                </tr>
            </tfoot>
        </table>
        '''
        
        soup = BeautifulSoup(html, 'html.parser')
        stats = scraper._parse_advanced_stats(soup)
        
        assert stats['player_efficiency_rating'] == "27.5"
        assert stats['true_shooting_pct'] == ".585"
        assert stats['box_plus_minus'] == "8.9"
        assert stats['win_shares'] == "214.0"
    
    def test_parse_bio_info(self, scraper):
        """Test parsing biographical information"""
        html = '''
        <div id="meta">
            <p>Position: Point Guard • Shoots: Right</p>
            <p>2003-2023</p>
        </div>
        '''
        
        soup = BeautifulSoup(html, 'html.parser')
        info = scraper._parse_bio_info(soup)
        
        assert info.get('position') == "Point Guard"
        assert info.get('years_active') == "2003-2023"
    
    def test_count_accolades(self, scraper):
        """Test counting championships and awards"""
        html = '''
        <div id="bling">
            <ul>
                <li>4× NBA MVP</li>
                <li>19× NBA All-Star</li>
                <li>18× All-NBA</li>
            </ul>
        </div>
        '''
        
        soup = BeautifulSoup(html, 'html.parser')
        accolades = scraper._count_accolades(soup)
        
        assert accolades['mvp_awards'] == 4
        assert accolades['all_star_selections'] == 19
        assert accolades['all_nba_selections'] == 18
    
    def test_save_player_data(self, scraper, mock_storage):
        """Test saving player data to storage"""
        player = PlayerStats(
            name="Test Player",
            player_id="testpl01",
            url="https://example.com",
            points_per_game=25.5
        )
        
        scraper.save_player_data(player)
        
        # Verify storage.write was called
        mock_storage.write.assert_called_once()
        
        # Check the call arguments
        call_args = mock_storage.write.call_args
        key = call_args[0][0]
        data = call_args[0][1]
        
        assert key == "bronze/players/testpl01.json"
        
        # Verify JSON is valid
        json_str = data.decode('utf-8')
        parsed = json.loads(json_str)
        assert parsed['name'] == "Test Player"
        assert parsed['points_per_game'] == 25.5
    
    @patch('src.ingestion.basketball_ref_scraper.BasketballReferenceScraper._make_request')
    def test_scrape_player_integration(self, mock_request, scraper):
        """Test full player scraping workflow"""
        # Create comprehensive mock HTML
        mock_html = '''
        <html>
            <div id="meta">
                <p>Position: Small Forward</p>
                <p>2003-Present</p>
            </div>
            <div id="bling">
                <ul>
                    <li>4× NBA MVP</li>
                    <li>19× NBA All-Star</li>
                </ul>
            </div>
            <table id="per_game">
                <tfoot>
                    <tr>
                        <th data-stat="season">Career</th>
                        <td data-stat="g">1421</td>
                        <td data-stat="pts_per_g">27.2</td>
                        <td data-stat="trb_per_g">7.5</td>
                        <td data-stat="ast_per_g">7.3</td>
                    </tr>
                </tfoot>
            </table>
            <table id="advanced">
                <tfoot>
                    <tr>
                        <th data-stat="season">Career</th>
                        <td data-stat="per">27.1</td>
                        <td data-stat="ts_pct">.586</td>
                        <td data-stat="bpm">8.8</td>
                        <td data-stat="ws">251.9</td>
                        <td data-stat="ws_per_48">.244</td>
                        <td data-stat="vorp">145.2</td>
                    </tr>
                </tfoot>
            </table>
        </html>
        '''
        
        mock_response = Mock()
        mock_response.content = mock_html.encode('utf-8')
        mock_request.return_value = mock_response
        
        player = scraper.scrape_player("jamesle01", "LeBron James")
        
        assert player is not None
        assert player.name == "LeBron James"
        assert player.player_id == "jamesle01"
        assert player.position == "Small Forward"
        assert player.games_played == 1421
        assert player.points_per_game == 27.2
        assert player.mvp_awards == 4
        assert player.all_star_selections == 19