CREATE TABLE station (
        id INTEGER NOT NULL,
        name VARCHAR(80) NOT NULL,
        code VARCHAR,
        longitude DECIMAL,
        latitude DECIMAL,
        url TEXT,
        region_id INTEGER,
        active INTEGER,
        PRIMARY KEY (id),
        UNIQUE (id),
        UNIQUE (name),
        UNIQUE (code),
        FOREIGN KEY(region_id) REFERENCES region (id)
);

CREATE TABLE measurement (
        id SERIAL,
        time TEXT,
        cs TEXT,
        "fof2" TEXT,
        "fof1" TEXT,
        "mufd" TEXT,
        "foes" TEXT,
        "foe" TEXT,
        "hf2" TEXT,
        "he" TEXT,
        "hme" TEXT,
        "hmf2" TEXT,
        "hmf1" TEXT,
        "yf2" TEXT,
        "yf1" TEXT,
        "tec" TEXT,
        "scalef2" TEXT,
        "fbes" TEXT,
        "altitude" TEXT,
        station_id INTEGER,
        PRIMARY KEY (id),
        UNIQUE (id),
        FOREIGN KEY(station_id) REFERENCES station (id)
);

BEGIN TRANSACTION;

INSERT INTO "region" VALUES(1,'test');
INSERT INTO "region" VALUES(2,'Asia');
INSERT INTO "region" VALUES(3,'Africa');
INSERT INTO "region" VALUES(4,'North America');
INSERT INTO "region" VALUES(5,'South America');
INSERT INTO "region" VALUES(6,'Europe');
INSERT INTO "region" VALUES(7,'Africa');
INSERT INTO "region" VALUES(8,'Australia');
INSERT INTO "station" VALUES(1,'Austin, TX, USA','AU930',262.3,30.4,'',4,NULL);
INSERT INTO "station" VALUES(2,'El Arenosillo, Spain','EA036',353.3,37.1,'',6,NULL);
INSERT INTO "station" VALUES(3,'Roquetes, Spain','EB040',0.5,40.8,'',6,NULL);
INSERT INTO "station" VALUES(4,'Athens, Greece','AT138',23.5,38,'',6,NULL);
INSERT INTO "station" VALUES(5,'Beijing, China','BP440',116.2,40.3,'',2,NULL);
INSERT INTO "station" VALUES(6,'Millstone Hill, MA, USA','MHJ45',288.5,42.6,'',4,NULL);
INSERT INTO "station" VALUES(7,'Pruhonice, Czech Republic','PQ052',14.6,50,'',6,NULL);
INSERT INTO "station" VALUES(8,'Sanya, China','SA418',109.42,18.34,'',2,NULL);
INSERT INTO "station" VALUES(9,'Yakutsk, Russia','YA462',129.6,62,'',2,NULL);
INSERT INTO "station" VALUES(10,'Juliusruh, Germany','JR055',13.4,54.6,'',6,NULL);
INSERT INTO "station" VALUES(11,'Boulder, CO, USA','BC840',254.7,40,'',4,NULL);
INSERT INTO "station" VALUES(12,'Dourbes, Belgium','DB049',4.6,50.1,'',6,NULL);
INSERT INTO "station" VALUES(13,'Boa Vista, Brazil','BVJ03',299.3,2.8,'',5,NULL);
INSERT INTO "station" VALUES(14,'Cachoeira Paulista, Brazil','CAJ2M',315,-22.7,'',5,NULL);
INSERT INTO "station" VALUES(15,'Fortaleza, Brazil','FZA0M',321.6,-3.9,'',5,NULL);
INSERT INTO "station" VALUES(16,'I-Cheon, South Korea','IC437',127.54,37.14,'',2,NULL);
INSERT INTO "station" VALUES(17,'Jeju Island, Korea','JJ433',126.3,33.43,'',2,NULL);
INSERT INTO "station" VALUES(18,'Nicosia, Cyprus','NI135',33.16,35.03,'',6,NULL);
INSERT INTO "station" VALUES(19,'Moscow, Russia','MO155',37.3,55.47,'',2,NULL);
INSERT INTO "station" VALUES(20,'Alpena MI, USA','AL945',276.44,45.07,'',4,NULL);
INSERT INTO "station" VALUES(21,'Chilton, United Kingdom','RL052',359.4,51.5,'',6,NULL);
INSERT INTO "station" VALUES(22,'Eglin AFB, FL, USA','EG931',273.5,30.5,'',4,NULL);
INSERT INTO "station" VALUES(23,'Eielson AFB AK, USA','EI764',212.93,64.66,'',4,NULL);
INSERT INTO "station" VALUES(24,'Fairford, England','FF051',358.5,51.7,'',6,NULL);
INSERT INTO "station" VALUES(25,'Grahamstown, South Africa','GR13L',26.5,-33.3,'',3,NULL);
INSERT INTO "station" VALUES(26,'Hermanus, South Africa','HE13N',19.22,-34.42,'',3,NULL);
INSERT INTO "station" VALUES(27,'Idaho Natl Lab, ID, USA','IF843',247.32,43.81,'',4,NULL);
INSERT INTO "station" VALUES(28,'Learmonth, Australia','LM42B',114.1,-21.8,'',8,NULL);
INSERT INTO "station" VALUES(29,'Louisvale, South Africa','LV12P',21.2,-28.5,'',3,NULL);
INSERT INTO "station" VALUES(30,'Melrose, FL, USA','ME929',278,29.71,'',4,NULL);
INSERT INTO "station" VALUES(31,'Pt Arguello, CA, USA','PA836',239.5,34.8,'',4,NULL);
INSERT INTO "station" VALUES(32,'Rome, Italy','RM041',12.5,41.8,'',6,NULL);
INSERT INTO "station" VALUES(33,'San Vito, Italy','VT139',17.8,40.6,'',6,NULL);
INSERT INTO "station" VALUES(34,'Tucuman, Argentina','TNJ20',294.6,-26.9,'',5,NULL);
INSERT INTO "station" VALUES(35,'Tromso, Norway','TR169',19.2,69.6,'',6,NULL);
INSERT INTO "station" VALUES(36,'Warsaw, Poland','MZ152',21.1,52.2,'',6,NULL);
INSERT INTO "station" VALUES(37,'Norfolk, Australia','NI63_',167.97,-29.03,'',8,NULL);
INSERT INTO "station" VALUES(38,'Canberra, Australia','CB53N',149,-35.32,'',8,NULL);
INSERT INTO "station" VALUES(39,'Camden, Australia','CN53L',150.67,-34.05,'',8,NULL);
INSERT INTO "station" VALUES(40,'Port Stanley, Falkland Islands','PSJ5J',302.1,-51.6,'',5,NULL);
INSERT INTO "station" VALUES(41,'Perth, Australia','PE43K',116.13,-32,'',8,NULL);
INSERT INTO "station" VALUES(42,'Darwin, Galápagos Islands','DW41K',130.95,-12.45,'',5,NULL);
INSERT INTO "station" VALUES(43,'Cocos Island','CS31K',96.83,-12.18,'',8,NULL);
INSERT INTO "station" VALUES(44,'Brisbane, Australia','BR52P',153.06,-27.06,'',8,NULL);
INSERT INTO "station" VALUES(45,'Hobart, Australia','HO54K',147.32,-42.92,'',8,NULL);
INSERT INTO "station" VALUES(46,'Townsville, Australia','TV51R',146.85,-19.63,'',8,NULL);
INSERT INTO "station" VALUES(47,'Sao Luis, Brazil','SAA0K',315.8,-2.6,'',5,NULL);
INSERT INTO "station" VALUES(48,'Madimbo, South Africa','MU12K',30.88,-22.39,'',3,NULL);
INSERT INTO "station" VALUES(49,'Guam, USA','GU513',144.86,13.62,'',8,NULL);
INSERT INTO "station" VALUES(50,'Campo Grande, Brazil','CGK21',305,-20.5,'',5,NULL);
INSERT INTO "station" VALUES(51,'Gibilmanna, Italy','GM037',14,37.9,'',6,NULL);
INSERT INTO "station" VALUES(52,'Jicamarca, Peru','JI91J',283.2,-12,'',5,NULL);
INSERT INTO "station" VALUES(53,'Wuhan, China','WU430',114.4,30.5,'',2,NULL);
INSERT INTO "station" VALUES(54,'Niue Island','ND61R',169.93,-19.07,'',8,NULL);
INSERT INTO "station" VALUES(55,'Mohe, China','MH453',122.52,52,'',2,NULL);
INSERT INTO "station" VALUES(56,'Ramey, Puerto Rico','PRJ18',292.9,18.5,'',4,NULL);
INSERT INTO "station" VALUES(57,'Gakona, AK, USA','GA762',215,62.38,'',4,NULL);
INSERT INTO "station" VALUES(58,'Santa Maria, Brazil','SMK29',307.81,-29.69,'',5,NULL);
INSERT INTO "station" VALUES(59,'Lualualei, Hawaii','LL721',201.85,21.43,'',4,NULL);
INSERT INTO "station" VALUES(60,'Belem, Brazil','BLJ03',311.56,1.43,NULL,5,NULL);
INSERT INTO "station" VALUES(61,'Wallops Island, VA, USA','WP937',284.5,37.9,NULL,4,NULL);
INSERT INTO "station" VALUES(62,'Ascension Island','AS00Q',345.6,-7.95,NULL,3,0);
INSERT INTO "station" VALUES(63,'Kiruna, Sweden','KI167',20.43,67.86,NULL,6,NULL);
INSERT INTO "station" VALUES(64,'Jang Bogo, Antartica','JB57N',164.24,-74.62,NULL,5,NULL);
INSERT INTO "station" VALUES(65,'Kent Island, Maryland, USA','KI939',283.72,38.96,NULL,4,NULL);
INSERT INTO "station" VALUES(66,'Sopron, Hungary','SO148',16.72,47.63,NULL,6,NULL);
INSERT INTO "station" VALUES(67,'EISCAT Tromso, Norway','TR170',19.22,69.58,NULL,6,NULL);
INSERT INTO "station" VALUES(68,'Eareckson Air Station','EA653',185.92,52.73,NULL,2,NULL);
INSERT INTO "station" VALUES(69,'Rome, Italy RO041','RO041',185.92,12.5,NULL,6,NULL);
COMMIT;
