from pathlib import Path
import re
from urllib import request


github_wcota = 'https://github.com/wcota/covid19br'
filename = re.compile(
    r'"/wcota/covid19br/blob/master/'
    r'cases-brazil-cities-time(\.|_\d+\.)csv\.gz"'
)


def pipeline_wcota(db):
    pamepi = Path().home().joinpath('PAMepi/') / db
    pamepi.mkdir(parents=True, exist_ok=True)

    with request.urlopen('https://github.com/wcota/covid19br') as wcota:
        for content in wcota:
            if (match := filename.search(content.decode())) is not None:
                endpoint = match.group().replace('"', '')
                link = github_wcota + '/raw' + endpoint.split('blob')[1]

                with request.urlopen(link) as csvgz:
                    with open(endpoint.split('/')[-1], 'wb') as f:
                        for content in csvgz:
                            f.write(content)
