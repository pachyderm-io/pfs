clean:
	pachctl delete pipeline map
	pachctl delete repo scraper

demo:
	pachctl create repo scraper
	pachctl put file scraper@master -f html2text.1.html
	pachctl update pipeline -f map.json --reprocess
