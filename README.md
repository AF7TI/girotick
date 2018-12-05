# girotick
fetch ionosphere data from Global Ionospheric Radio Observatory lgdc.uml.edu DIDBGetValues

Dockerfile launches tick.py which creates PostgreSQL db on localhost, initializes schema, populates with active stations and gets GIRO data every 5 mins thereafter

backend for https://github.com/AF7TI/girotick
