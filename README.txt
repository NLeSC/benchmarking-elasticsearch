convert_to_json.py
	Dit Python script is afgeleid van import_kb.py en converteert de 
	XML bestanden (de output van process_kb.py) tot .json bestanden.
	Dit script neemt drie argumenten input_dir log_dir json_dir,
	welke vergelijkbaar zijn aan import_kb.py, behalve de json_dir.
	Aangezien dit script enkel XML naar JSON converteert, moet dit
	ergens worden opgeslagen dus dit gebeurt in de json_dir.

import_json.sh
	Dit script laadt de JSON bestanden een voor een in via een
	eenvoudige for-loop en curl op de bulk API.

optimizekbs.sh
	Dit script is een voorbeeld van het aanroepen van de optimize
	API op een bepaalde index.
	
par.sh / par_remote.sh
	Deze scripts maken gebruik van parallel om meerdere JSON bestanden
	tegelijkertijd de bulk API in te duwen. Let op, deze scripts zijn
	nog niet generiek. Deze scripts maken gebruik van processOne.sh en
	respectievelijk processOne_remote.sh
	
processOne.sh / processOne_remote.sh
	Deze scripts worden gebruikt door par.sh en respectievelijk
	par_remote.sh om één van de JSON bestanden uit te pakken en door
	de bulk API te laten verwerken.

mapreduce1/mapred?.java
	Dit zijn de verschillende iteraties van de mapreduce versie van
	het genereren van wordclouds. Merk op dat dit dus geen gebruik maakt
	van de beschikbare termvectors aangezien dit niet in queryvorm past:
		mapred1.java -> Eerste eenvoudige poging voor proof-of-concept
		mapred2.java -> Een iets generiekere versie, niet bijzonder
		mapred3.java -> Andrea's top-100 implementatie gebruikt welke
		                gebruik maakt van een Combiner
		mapred4.java -> Maakt gebruik van de cleanup methode bij de
		                Mapper, zodat je niet voor iedere term
						collect aan hoeft te roepen.
	    mapred5.java -> Haalt toch de cleanup methode bij de Mapper
						weer weg aangezien dit toch niet voordelig was.
						Voegt meer logic in welke ook in de Python
						versie wordt toegepast (minstens 2 letters).
						Let op dat de comment op regel 93 suggereert
						dat er gebruik wordt gemaakt van het geheugen
						limiet, maar hier heb ik uiteindelijk vanaf
						gezien en ik heb een arbitraire waarde
						gekozen om vanaf dat punt alle redundante
						woorden alvast te verwijderen. Deze methode is
						uiteindelijk gebruikt voor de benchmarks. 
		mapred6.java -> Voert een totaal andere mapreduce uit, namelijk
						om te kijken hoeveel woorden er gemiddeld per
						document zijn en hoeveel documenten er per query
						matchen om inzicht te krijgen in de queries.
	ElasticSearch-Hadoop is nodig om hier gebruik van te maken:
	https://www.elastic.co/products/hadoop
	Ik heb zelf alle dependencies in één jar gegooid zodat ik in Hadoop
	niet de references in hoefde te stellen, maar dat is natuurlijk ook
	mogelijk.
	
queries/queries.java 
	Dit bestand is gebruikt om te kijken of de Python implementatie
	trager was dan een Java implementatie aangezien de Python implementatie
	gebruik maakt van de REST API en Java werkelijk deelneemt aan het
	cluster en de interne communicatie gebruikt. De Java versie bouwt op
	een vergelijkbare manier de query op als in de Python versie. Echter,
	er zit wel een aantal onhandige dingen in aangezien het zeer slecht
	gedocumenteerd is wat je precies moet gebruiken. Het functioneert dus
	wel, maar kan hoogstwaarschijnlijk wel iets netter. Voornamelijk de
	excludes zijn vrij raar herschreven aangezien ik dacht dat hier een
	fout in zat, maar uiteindelijk bleek de fout ergens anders te zitten en
	heb ik de moeite niet genomen om de herschreven versie weer te
	vereenvoudigen, het werkte immers.
	
----
De volgende scripts maken allemaal gebruik van de beschikbare user queries.
Deze queries zijn voor het gemak in een sqlite database gestopt, welke
te vinden is als queries.db of enkel de queries in queries.sql. Ieder script
haalt uit de database de query gegevens en stopt de execution times terug
in de/een database.
----
	
wordcloud.hadoop.py / wordcloud.hadoop.sh
	Dit script zorgt ervoor dat de hadoop versie van de wordcloud generation
	wordt uitgevoerd. Er zit hierin nog een aantal dingen wat hardcoded is,
	maar dit is eenvoudig om te zetten tot parameters en voor een productie
	omgeving moet er sowieso het een en ander worden aangepast. Het
	belangrijkste zit hem erin dat de hadoop versie een hele JSON string
	meekrijgt als query.

wordcloud.java.py / wordcloud.java.sh
	Dit script zorgt ervoor dat de java versie van de wordcloud generation
	wordt uitgevoerd. Deze krijgt wel alle gegevens van de query los en
	bouwt vervolgens zelf een query op.
	
wordcloud.py / wordcloud.sh
	Dit script voert de Python versie uit. Deze code is uit de Texcavator
	source gehaald en er is ook geexperimenteerd met het gebruik van de
	scroll methode in Python.
	
	
	
	
	
	