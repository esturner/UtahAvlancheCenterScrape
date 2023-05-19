# Utah Avlanche Center Scrape
Webscrape of the Utah avalanche website for data exploration and practice with predictors.

## Introduction

The main idea of this project is to generate a well calibrated avalanche forecast based the observations of winter conditions typically performed by backcountry skiers and snowboarders, the local weather, and avalanche reports. These forecasts provide very valuable information for not only backcountry users but also highway management in mountainous terrain. Currently forecasts are made by avalanche professionals from regional avalanche centers which are supported by public fundraisers and government support. This avalanche project uses observations from the Utah Avalanche Center (UAC) Website (https://utahavalanchecenter.org) which hosts reports that cover the entire state. 

## Web Scraping

**scrape.py Packages**  - requests, BeautifulSoup, Pandas, matplotlib

To gather data we first scrape the Utah Avalanche Center website (https://utahavalanchecenter.org) for all avalanche reports, general observations, and avalanche forecasts. This process is carried out using the functions built up in scrape.py.
