# girotick
Fetch ionosphere data from Global Ionospheric Radio Observatory lgdc.uml.edu DIDBGetValues and store in postgres. Backend for [giroapi](https://github.com/AF7TI/giroapi)

## Installation

Widespread usage is strongly discouraged because GIRO doesn't need a ton of database connections. Please consume data from an existing [giroapp webservice](https://github.com/AF7TI/giroapp#running-code)

Build this image from Dockerfile, tag with girotick   
    `docker build -t girotick .`

## Configuration
Pass database connection info through environment variables:  
    `docker run -e DB_USER="postgres" -e DB_HOST="127.0.0.1" -e DB_NAME="postgres" -e DB_PASSWORD="mysecretpassword" tick`

Dockerfile launches tick.py which initializes postgres schema, populates with active stations and gets GIRO data thereafter

## Contributing
Contributions welcome! Forgive my intial barbaric use of github. Fork the project and open a pull request.

## Thank you
Thanks to [@arodland](https://github.com/arodland/girotick) for working with GIRO to implement performance improvements.

Data made available through [UMass Lowell Global Ionospheric Radio observatory (GIRO)](http://umlcar.uml.edu/DIDBase/RulesOfTheRoadForDIDBase.htm)
