# girotick
fetch ionosphere data from Global Ionospheric Radio Observatory lgdc.uml.edu DIDBGetValues

Dockerfile launches tick.py which creates and populates PostgreSQL db if it doesnt exist and gets GIRO data every 5 mins thereafter
