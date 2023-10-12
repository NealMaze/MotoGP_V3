import urllib3
import unicodedata
from queue import Queue
import threading
import logging
import datetime
import utils


from scraper_request import ScraperRequest


class MotoGP(ScraperRequest):

    """
    Scrape data from https://www.motogp.com/en/gp-results
    """
    def __init__(self, use_proxy=True):
        requests_config = {
            'use_proxy': use_proxy,
            'proxy_type': 'static',
            'min_wait': 5,
            'max_wait': 15,
            'min_requests': 5,
            'max_requests': 10,
            'read_timeout': 180,
            'thread_lock': threading.Lock(),

        }
        super(MotoGP, self).__init__(**requests_config)
        self.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })

        self.test_page = 'false'

    def get_seasons(self):
        """
        Get the ID for each available season
        :return: dict
        """
        resp = self.make_request(
            'GET',
            f'https://www.motogp.com/api/results-front/be/results-api/seasons?test={self.test_page}',
            return_json=True
        )

        # return a dict with year as key and season id as value
        return {
            season['year']: season['id'] for season in resp
        }

    def get_events(self, season_id):
        """
        Get the list of events from a given season
        :param season_id: str
        :return: dict
        """
        events = {}

        resp = self.make_request(
            'GET',
            f'https://www.motogp.com/api/results-front/be/results-api/'
            f'season/{season_id}/events?finished=true&test={self.test_page}',
            return_json=True
        )

        for index, event in enumerate(resp):
            events[index+1] = {
                'id': event['id'],
                'round': str(index+1).zfill(2),
                'name': event['name'],
                'circuit_information': event['event_files']['circuit_information']['url']
            }

        return events

    def get_leagues(self, event):
        """
        Get the available leagues for a given event
        :param event: dict with event details
        :return: list of dicts
        """
        leagues = []

        resp = self.make_request(
            'GET',
            f'https://www.motogp.com/api/results-front/be/results-api/event/{event["id"]}/categories',
            return_json=True
        )

        for league in resp:
            result = dict(event)
            result.update({
                'league_id': league['id'],
                'league_name': league['name']
            })

            leagues.append(result)

        return leagues

    def get_sessions(self, event):
        """
        Get all seassions for a given league's event
        :param event: dict with event and league details
        :return: list of dicts
        """
        results = []
        resp = self.make_request(
            'GET',
            f'https://www.motogp.com/api/results-front/be/results-api/event/'
            f'{event["id"]}/category/{event["league_id"]}/sessions',
            return_json=True
        )

        for session in resp:

            session_type = session['type']

            if session['number']:
                session_type += str(session['number'])

            results.append(
                {
                    'Year': session['date'].split('-')[0],
                    'Round': event['round'],
                    'RoundName': event['name'],
                    'League': event['league_name'],
                    'SessionType': session_type,
                    'SessionDate': session['date'],
                    'SessionStatus': session['status'],
                    'SessionCircuit': session['circuit'],
                    'Track': session['condition']['track'],
                    'Air': session['condition']['air'],
                    'Humidity': session['condition']['humidity'],
                    'Ground': session['condition']['ground'],
                    'Weather': session['condition']['weather'],
                    'files': {
                        'Analysis': session['session_files']['analysis']['url'],
                        'AverageSpeed': session['session_files']['average_speed']['url'],
                        'FastLap': session['session_files']['fast_lap_sequence']['url'],
                        'LapChart': session['session_files']['lap_chart']['url'],
                        'AnalByLap': session['session_files']['analysis_by_lap']['url'],
                        'FastLapRider': session['session_files']['fast_lap_rider']['url'],
                        'Grid': session['session_files']['grid']['url'],
                        'Session': session['session_files']['session']['url'],
                        'CircuitInfo': event.get('circuit_information')  # already got it from get_events() call
                    }
                }
            )

        return results

    def download_files(self, session):
        """
        Download files for a given session
        :param session: dict
        :return:
        """
        if self.test_page == 'true':
            extension = '_TEST.pdf'  # to differentiate the files gp-results to test-results scraping

        else:
            extension = '.pdf'

        for file_type, file_url in session['files'].items():

            if not file_url:
                # file doesnt exists
                continue

            try:

                fn = f"{session['Year']}-{session['League']}-Round_{session['Round']}-{session['SessionType']}-{file_type}{extension}"
                # remove special characters
                fn = unicodedata.normalize('NFKD', fn).encode("ascii", "replace").replace(b'?', b'')

                utils.download_file(
                    self,
                    file_url,
                    fn
                )

            except Exception as e:
                logging.exception(f'download_files[{e}]:{session}')


def main():
    start = datetime.datetime.now()
    results = Queue()
    scraper = MotoGP()
    page_type = input("Page type[test/gp]:").strip().lower()
    year = input('Year:').strip()
    round = input('Round:')

    if page_type not in ['test', 'gp']:
        print('Invalid page type. Enter test or gp.')
        return

    if page_type == 'test':
        scraper.test_page = 'true'

    else:
        scraper.test_page = 'false'

    headers = [
        'Year', 'Round', 'RoundName', 'League', 'SessionType', 'SessionDate', 'SessionStatus',
        'SessionCircuit', 'Track', 'Air', 'Humidity', 'Ground', 'Weather'
    ]

    seasons = scraper.get_seasons()

    try:

        if len(year) != 4:
            raise

        year = int(year)

    except:
        print('Year must be a 4 digits number.')
        return

    if year not in seasons:
        years = sorted(list(seasons.keys()), reverse=True)
        print(f'Year must be between {years[-1]} and {years[0]}')
        return

    try:
        round = int(round)
    except:
        print('Round must be a 1 or 2 digits number.')
        return

    events = scraper.get_events(seasons[year])

    if round not in events:
        rounds = sorted(list(events.keys()), reverse=True)
        print(f'Year must be between {rounds[-1]} and {rounds[0]}')
        return

    leagues = scraper.get_leagues(events[round])

    for league in leagues:
        print(f"Scraping {year} - {league['round']} - {league['name']} - {league['league_name']}")
        try:
            sessions = scraper.get_sessions(league)
            print("Downloading files")

            for session in sessions:
                scraper.download_files(session)
                session.pop('files')  # remove files data from end result

                results.put(session)

        except Exception as e:
            logging.exception(f'MAIN[{e}]:{year}:{league}')

    # write results to csv
    utils.write_to_csv(results, f'{year}-SessionWeather.csv', headers, filemode='a')

    print("Done in", datetime.datetime.now() - start)

if __name__ == '__main__':
    urllib3.disable_warnings()
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("retry").setLevel(logging.ERROR)
    logging.basicConfig(level=logging.ERROR)

    logging.basicConfig(level=logging.WARNING,
                        filename=f'motogp.log', filemode='w')

    try:
        main()
    except Exception as e:
        logging.exception(f'MAIN[{e}]')
