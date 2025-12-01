from datetime import datetime, timedelta
import cartopy.crs as ccrs
import requests
import pandas as pd
import siphon.catalog as TDSCatalog
from metpy.cbook import get_test_data
import metpy.plots as mpplots
import io

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#LINK EXAMPLE
  # https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?

  # ---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---METAR---

  # data=all&year1=0000&month1=0&day1=0&year2=0000&month2=0&day2=0&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#DATA PULLING START DATE  (FORMAT: year1 = 0000, month1 = 0/00, day1 = 0/00)

year1   = 2025
month1  = 11
day1    = 28

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#DATA PULLING END DATE    (PULLS DATA FROM SPECIFIC MONTH OF A YEAR)

year2   = year1
month2  = 12

#DAY (IF STATEMENT ACCOUNTS FOR DIFFERENT DAYS IN EACH MONTH, DEPENDING ON MONTH AND YEAR)
day2    = 1

#FEBUARY
if month2 == 2:
  if year2 % 4 == 0:
    if year2 == 2000:
      day2 = day2
    else:
      day2 = day2 +1
  else:
    day2   = day2

#JANUARY, MARCH, MAY, JULY, AUGUST, OCTOBER, DECEMBER
elif month2 in [1,3,5,7,8,10,12]:
  day2    = day2 +3

#APRIL, JUNE, SEPTEMBER, NOVEMBER
elif month2 in [4,6,9,11]:
  day2    = day2 +2

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#PLACE HOLDER METAR (ACCOUNTS FOR ARRAY INDEX OFFSET)

#MIZZOU METEOROLOGY - CODE BUILT BY DR. BEACH AND NERDY MCCURDY
metar_url_place_holder =              'station=COU&station=UNO&station=SPI'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#NORTH AMERICAN STATIONS

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CARIBBEAN (COUNTRIES & TERRITORIES --- NOT ALL ARE AVAILABLE)

#CARIBB: 1
metar_url_antigua_barbuda =           'station=TAPA&station=TKPN&'

#CARIBB: 2
metar_url_british_virgin_islands =    'station=TUPJ&'

#CARIBB: 3
metar_url_cuba =                      'station=MUBY&station=MUCA&station=MUCF&station=MUCL&station=MUCM&station=MUCU&station=MUGM&station=MUGT&station=MUHA&station=MUMO&station=MUMZ&station=MUNG&station=MUSA&station=MUVR&station=MUVT&'

#CARIBB: 4
metar_url_dominica =                  'station=TFFR&'

#CARIBB: 5
metar_url_haiti =                     'station=MTCH&station=MTPP&'

#CARIBB: 6
metar_url_jamaica =                   'station=MKJP&station=MKJS&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CANADA (PROVINCES & TERRITORIES)

#CAN: 1
metar_url_alberta =                   'station=CWLB&station=CWRM&station=CWZG&station=CYBW&station=CYED&station=CYEG&station=CYET&station=CYOD&station=CYOJ&station=CYPE&station=CYQF&station=CYQU&station=CYXD&station=CYYC&station=CYZH&station=CYZU&'

#CAN: 2
metar_url_british_columbia =          'station=CWCL&station=CWEB&station=CWDL&station=CWEZ&station=CWHC&station=CWJU&station=CWJV&station=CWKV&station=CWLM&station=CWLY&station=CWPU&station=CWPZ&station=CWQS&station=CWSW&station=CWZA&station=CWZV&station=CYAZ&station=CYBL&station=CYCG&station=CYCP&station=CYCD&station=CYDC&station=CYGE&station=CYKA&station=CYPR&station=CYQQ&station=CYQZ&station=CYVR&station=CYWL&station=CYXC&station=CYXJ&station=CYXS&station=CYXT&station=CYXX&station=CYYD&station=CYYE&station=CYYF&station=CYYJ&station=CYZT&station=CZST&'

#CAN: 3
metar_url_manitoba =                  'station=CWJD&station=CWPO&station=CYBR&station=CYGX&station=CYIV&station=CYNE&station=CYQD&station=CYTH&station=CYWG&station=CYPG&station=CYYL&'

#CAN: 4
metar_url_new_brunswick =             'station=CYCX&station=CYFC&station=CYQM&station=CYSJ&station=CWSS&'

#CAN: 5
metar_url_newfoundland =              'station=CWAR&station=CWCA&station=CWHO&station=CWRA&station=CWWU&station=CYAY&station=CYCA&station=CYDF&station=CYJT&station=CYMH&station=CYQX&station=CYWK&station=CYYR&station=CYYT&'

#CAN: 6
metar_url_northwest_territories =     'station=CYCO&station=CYEV&station=CYFR&station=CYFS&station=CYGH&station=CYHI&station=CYHY&station=CYKD&station=CYSM&station=CYSY&station=CYUB&station=CYVQ&station=CYWY&station=CYZF&station=CZCP&station=CZFM&station=CZFN&'

#CAN: 7
metar_url_nova_scotia =               'station=CYQI&station=CYQY&station=CYZX&station=CYAW&station=CYHZ&station=CXCH&station=CWWE&'

#CAN: 8
metar_url_nunavut =                   'station=CWEU&station=CWFD&station=CWGZ&station=CWJC&station=CWLX&station=CWOB&station=CWUP&station=CWUW&station=CWVD&station=CYBB&station=CYBK&station=CYCB&station=CYCS&station=CYCY&station=CYFB&station=CYGT&station=CYIO&station=CYLC&station=CYLT&station=CYRB&station=CYUS&station=CYUT&station=CYUX&station=CYVM&station=CYXN&station=CYXP&station=CYYH&station=CYZS&'

#CAN: 9
metar_url_ontario =                   'station=CWCI&station=CWLS&station=CWNC&station=CZSJ&station=CYYZ&station=CYXU&station=CYXZ&station=CYYB&station=CYYU&station=CYWA&station=CYXL&station=CYTR&station=CYTS&station=CYTZ&station=CYVV&station=CYQA&station=CYQG&station=CYQK&station=CYQT&station=CYRL&station=CYSB&station=CYPQ&station=CYOW&station=CYPL&station=CYLD&station=CYHD&station=CYEL&station=CXET&station=CWWZ&station=CWQP&'

#CAN: 10
metar_url_prince_edward_island =      'station=CWSD&station=CYYG&station=CWEP&'

#CAN: 11
metar_url_quebec =                    'station=CWBY&station=CWDM&station=CWNH&station=CWPK&station=CWQV&station=CYAD&station=CYBG&station=CYBX&station=CYGL&station=CYGP&station=CYGR&station=CYGW&station=CYKG&station=CYKL&station=CYHU&station=CYLA&station=CYMT&station=CYMX&station=CYOY&station=CYRJ&station=CYSC&station=CYUL&station=CYUY&station=CYVO&station=CYVP&station=CYYY&station=CYZV&'

#CAN: 12
metar_url_saskatchewan =              'station=CWDC&station=CWEH&station=CWIK&station=CWKO&station=CWOY&station=CWVT&station=CYBU&station=CYEN&station=CYKY&station=CYLJ&station=CYMJ&station=CYPA&station=CYQR&station=CYQV&station=CYQW&station=CYVC&station=CYVT&station=CYXE&'

#CAN: 13
metar_url_yukon =                     'station=CWZW&station=CYDA&station=CYMA&station=CYOC&station=CYQH&station=CYUA&station=CYXQ&station=CYXY&station=CYZW&'

#GRL: 1
metar_url_greenland =                 'station=BGBW&station=BGKK&station=BGSF&station=BGTL&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CENTRAL AMERICA (COUNTRIES & TERRITORIES)

#CAM: 1
metar_url_belize =                    'station=COU&'

#CAM: 2
metar_url_costa_rica =                'station=COU&'

#CAM: 3
metar_url_el_salvador =               'station=COU&'

#CAM: 4
metar_url_guatemala =                 'station=COU&'

#CAM: 5
metar_url_honduras =                  'station=COU&'

#CAM: 6
metar_url_nicaragua =                 'station=COU&'

#CAM: 7
metar_url_panama =                    'station=COU&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#MEXICO (STATES)

#MEX: 1
metar_url_aguascalientes =            'station=COU&'

#MEX: 2
metar_url_baja_california =           'station=COU&'

#MEX: 3
metar_url_baja_california_sur =       'station=COU&'

#MEX: 4
metar_url_campeche =                  'station=COU&'

#MEX: 5
metar_url_chiapas =                   'station=COU&'

#MEX: 6
metar_url_chihuahua =                 'station=COU&'

#MEX: 7
metar_url_coahuila =                  'station=COU&'

#MEX: 8
metar_url_colima =                    'station=COU&'

#MEX: 9
metar_url_durango =                   'station=COU&'

#MEX: 10
metar_url_guanajuato =                'station=COU&'

#MEX: 11
metar_url_guerrero =                  'station=COU&'

#MEX: 12
metar_url_hidalgo =                   'station=COU&'

#MEX: 13
metar_url_jalisco =                   'station=COU&'

#MEX: 14
metar_url_mexico =                    'station=COU&'

#MEX:15
metar_url_mexico_city =               'station=COU&'

#MEX: 16
metar_url_michoacan =                 'station=COU&'

#MEX: 17
metar_url_morelos =                   'station=COU&'

#MEX: 18
metar_url_nayarit =                   'station=COU&'

#MEX: 19
metar_url_nuevo_leon =                'station=COU&'

#MEX: 20
metar_url_oaxaca =                    'station=COU&'

#MEX: 21
metar_url_puebla =                    'station=COU&'

#MEX: 22
metar_url_queretaro =                 'station=COU&'

#MEX: 23
metar_url_quintana_roo =              'station=COU&'

#MEX: 24
metar_url_san_luis_potosi =           'station=COU&'

#MEX: 25
metar_url_sinaloa =                   'station=COU&'

#MEX: 26
metar_url_sonora =                    'station=COU&'

#MEX: 27
metar_url_tabasco =                   'station=COU&'

#MEX: 28
metar_url_tamaulipas =                'station=COU&'

#MEX: 29
metar_url_tlaxcala =                  'station=COU&'

#MEX: 30
metar_url_veracruz =                  'station=COU&'

#MEX: 31
metar_url_yucatan =                   'station=COU&'

#MEX: 32
metar_url_zacatecas =                 'station=COU&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#UNITED STATES (CONTIGUOUS)

#CONUS: 1
metar_url_alabama =                   'station=79J&station=ANB&station=BFM&station=BHM&station=DHN&station=GAD&station=HSV&station=MGM&station=MOB&station=MSL&station=MXF&station=OZR&'

#CONUS: 2
metar_url_arizona =                   'station=DMA&station=DUG&station=FHU&station=FLG&station=GBN&station=GCN&station=GXF&station=GYR&station=HII&station=IGM&station=INW&station=IWA&station=LUF&station=NYL&station=P08&station=PAN&station=PGA&station=PHX&station=PRC&station=SAD&station=TUS&station=YUM&'

#CONUS: 3
metar_url_arterritory =               'station=ADF&station=ARG&station=AWM&station=BYH&station=ELD&station=FLP&station=FSM&station=FYV&station=HOT&station=HRO&station=JBR&station=LIT&station=LRF&station=LZK&station=PBF&station=TXK&'

#CONUS: 4
metar_url_california =                'station=87Q&station=ACV&station=APC&station=AVX&station=BAB&station=BFL&station=BIH&station=BLH&station=BLU&station=BUO&station=BUR&station=CCR&station=CEC&station=CIC&station=CMA&station=CNO&station=CRQ&station=CVH&station=CZZ&station=DAG&station=EDW&station=EED&station=EKA&station=FAT&station=FCH&station=FUL&station=HHR&station=HWD&station=IPL&station=IYK&station=LAX&station=LGB&station=MCC&station=MER&station=MHR&station=MHS&station=MOD&station=MRY&station=MWS&station=MYF&station=MYV&station=NFG&station=NID&station=NJK&station=NKX&station=NLC&station=NRS&station=NSI&station=NTD&station=NUC&station=NUQ&station=NXF&station=NXP&station=NZJ&station=NZY&station=O87&station=OAK&station=ONT&station=OXR&station=PMD&station=POC&station=PRB&station=PSP&station=RAL&station=RBL&station=RDD&station=SAC&station=SAN&station=RIV&station=SBA&station=SBD&station=SBP&station=SCK&station=SDB&station=SDM&station=SEE&station=SFO&station=SIY&station=SJC&station=SLI&station=SMF&station=SMO&station=SMX&station=SNA&station=SNS&station=STS&station=SUU&station=SVE&station=TOA&station=TRK&station=TRM&station=TSP&station=TVL&station=UKI&station=VBG&station=VCV&station=VNY&station=VIS&station=WJF&'

#CONUS: 5
metar_url_colorado =                  'station=4BM&station=AFF&station=AKO&station=ALS&station=ASE&station=BKF&station=CAG&station=CEZ&station=COS&station=CPW&station=DEN&station=DRO&station=EEO&station=EGE&station=FCS&station=GJT&station=GUC&station=HDN&station=HEQ&station=LHX&station=LIC&station=LXV&station=MTJ&station=PUB&station=TAD&'

#CONUS: 6
metar_url_connecticut =               'station=BDL&station=BDR&station=DXR&station=GON&station=HFD&station=HVN&station=MMK&'

#CONUS: 7
metar_url_delaware =                  'station=DOV&station=ILG&'

#CONUS: 8
metar_url_florida =                   'station=42J&station=AAF&station=APF&station=BOW&station=CEW&station=COF&station=CRG&station=CTY&station=DAB&station=EGI&station=EYW&station=FLL&station=FMY&station=GNV&station=HRT&station=HST&station=JAX&station=LAL&station=MAI&station=MCF&station=MCO&station=MIA&station=MLB&station=NIP&station=NPA&station=NQX&station=NRB&station=NSE&station=OCF&station=OMN&station=OPF&station=ORL&station=PAM&station=PBI&station=PFN&station=PIE&station=PNS&station=SEF&station=SFB&station=SRQ&station=TIX&station=TLH&station=TMB&station=TPA&station=VNC&station=VPS&station=VRB&station=XMR&'

#CONUS: 9
metar_url_georgia =                   'station=ABY&station=AGS&station=AHN&station=AMG&station=ATL&station=AYS&station=BQK&station=CSG&station=FTY&station=LHW&station=LSF&station=MCN&station=MGE&station=MGR&station=RMG&station=SAV&station=SSI&station=SVN&station=VAD&station=VLD&station=WRB&'

#CONUS: 10
metar_url_idaho =                     'station=BOI&station=BYI&station=COE&station=IDA&station=LWS&station=MLD&station=MLP&station=MUO&station=PIH&station=SMN&station=SUN&station=TWF&'

#CONUS: 11
metar_url_illinois =                  'station=ALN&station=BLV&station=BMI&station=CGX&station=CMI&station=CPS&station=DEC&station=DKB&station=DNV&station=DPA&station=JOT&station=MDH&station=MDW&station=MLI&station=MTO&station=MVN&station=MWA&station=ORD&station=PIA&station=RFD&station=SLO&station=SPI&station=UIN&'

#CONUS: 12
metar_url_indiana =                   'station=BAK&station=BMG&station=EVV&station=FWA&station=GSH&station=GUS&station=HUF&station=IND&station=LAF&station=MGC&station=MIE&station=SBN&'

#CONUS: 13
metar_url_iowa =                      'station=AIO&station=ALO&station=BNW&station=BRL&station=CID&station=DBQ&station=DSM&station=FOD&station=LWD&station=MCW&station=OTM&station=SPW&station=SUX&'

#CONUS: 14
metar_url_territory =                 'station=CBK&station=CNU&station=CNK&station=DDC&station=EMP&station=FLV&station=FOE&station=FRI&station=GBD&station=GCK&station=GLD&station=HLC&station=HUT&station=IAB&station=ICT&station=IXD&station=LBL&station=MHK&station=NRN&station=OIN&station=OJC&station=P28&station=PHG&station=PTT&station=RSL&station=SLN&station=SYF&station=TOP&'

#CONUS: 15
metar_url_kentucky =                  'station=BWG&station=CEY&station=CVG&station=FTK&station=HOP&station=JKL&station=LEX&station=LOU&station=LOZ&station=OWB&station=PAH&station=SDF&'

#CONUS: 16
metar_url_louisiana =                 'station=7R3&station=7R4&station=AEX&station=ARA&station=BAD&station=BTR&station=BVE&station=CWF&station=ESF&station=GAO&station=HUM&station=LCH&station=LFT&station=MLU&station=MSY&station=NBG&station=NEW&station=POE&station=SHV&station=SRN&'

#CONUS: 17
metar_url_maine =                     'station=AUG&station=BGR&station=BHB&station=CAR&station=EPM&station=GNR&station=HUL&station=MLT&station=NHZ&station=OLD&station=PQI&station=PWM&station=RKD&station=SFM&'

#CONUS: 18
metar_url_maryland =                  'station=ADW&station=APG&station=BWI&station=CGS&station=FDK&station=FME&station=HGR&station=MTN&station=NAK&station=NHK&station=SBY&'

#CONUS: 19
metar_url_massachusetts =             'station=ACK&station=AQW&station=BAF&station=BED&station=BOS&station=BVY&station=CEF&station=CQX&station=EWB&station=FIT&station=FMH&station=HYA&station=LWM&station=MVY&station=ORE&station=ORH&station=OWD&station=PSF&station=TAN&'

#CONUS: 20
metar_url_michigan =                  'station=ANJ&station=APN&station=ARB&station=AZO&station=BEH&station=BTL&station=CAD&station=CIU&station=CMX&station=DET&station=DTW&station=ESC&station=FNT&station=GRR&station=HTL&station=IKW&station=IMT&station=IWD&station=JXN&station=LAN&station=MBL&station=MBS&station=MCD&station=MGN&station=MKG&station=MNM&station=MTC&station=OSC&station=OZW&station=P58&station=P59&station=PLN&station=PTK&station=RNP&station=SAW&station=TVC&station=YIP&'

#CONUS: 21
metar_url_minnesota =                 'station=AXN&station=BJI&station=BRD&station=DLH&station=DTL&station=DYT&station=FRM&station=GHW&station=HIB&station=INL&station=JMR&station=MKT&station=MSP&station=MZH&station=OTG&station=RST&station=RWF&station=STC&station=STP&station=TVF&'

#CONUS: 22
metar_url_mississippi =               'station=BIX&station=CBM&station=GLH&station=GPT&station=GTR&station=GWO&station=HBG&station=HEZ&station=HKS&station=HSA&station=IDL&station=JAN&station=MCB&station=MEI&station=NMM&station=OLV&station=PIB&station=PQL&station=TUP&station=UOX&'

#CONUS: 23
metar_url_missouri =                  'station=AIZ&station=CGI&station=COU&station=FAM&station=IRK&station=JEF&station=JLN&station=MCI&station=MKC&station=POF&station=SGF&station=STJ&station=STL&station=SUS&station=SZL&station=TBN&station=UNO&station=VIH&'

#CONUS: 24
metar_url_montana =                   'station=3DU&station=3HT&station=3TH&station=BIL&station=BTM&station=BZN&station=CTB&station=DLN&station=GDV&station=GFA&station=GGW&station=GPI&station=GTF&station=HLN&station=HVR&station=JDN&station=LVM&station=LWT&station=MLS&station=MSO&station=OLF&station=RPX&station=SDY&station=WEY&station=WYS&'

#CONUS: 25
metar_url_nebraska =                  'station=AIA&station=ANW&station=BBW&station=BFF&station=BIE&station=CDR&station=EAR&station=GRI&station=GRN&station=HDE&station=HSI&station=IML&station=LBF&station=LNK&station=LXN&station=MCK&station=OFF&station=OFK&station=OLU&station=OMA&station=ONL&station=SNY&station=VTN&'

#CONUS: 26
metar_url_nevada =                    'station=B23&station=BAM&station=DRA&station=EKO&station=ELY&station=INS&station=LOL&station=LSV&station=NFL&station=P38&station=P68&station=RNO&station=TPH&station=U31&station=WMC&'

#CONUS: 27
metar_url_new_hampshire =             'station=CON&station=EEN&station=LCI&station=LEB&station=MHT&station=MWN&station=PSM&'

#CONUS: 28
metar_url_new_mexico =                'station=4CR&station=4MR&station=4SL&station=ABQ&station=ATS&station=CAO&station=CNM&station=CVS&station=DMN&station=FMN&station=GNT&station=GUP&station=HMN&station=HOB&station=LRU&station=LVS&station=ONM&station=ROW&station=RTN&station=SAF&station=SVC&station=TCC&station=TCS&'

#CONUS: 29
metar_url_new_jersey =                'station=ACY&station=BLM&station=EWR&station=MIV&station=NEL&station=TEB&station=TTN&station=WRI&'

#CONUS: 30
metar_url_new_york =                  'station=ALB&station=ART&station=BGM&station=DKK&station=BUF&station=DSV&station=ELM&station=ELZ&station=FOK&station=FRG&station=GFL&station=GTB&station=HPN&station=IAG&station=ISP&station=ITH&station=JFK&station=JHW&station=LGA&station=MSS&station=MTP&station=NYC&station=OGS&station=PBG&station=PLB&station=POU&station=RME&station=ROC&station=SCH&station=SLK&station=SWF&station=SYR&station=UCA&'

#CONUS: 31
metar_url_north_carolina =            'station=AVL&station=CLT&station=ECG&station=EWN&station=FAY&station=FBG&station=GSB&station=GSO&station=HFF&station=HKY&station=HSE&station=ILM&station=INT&station=ISO&station=LBT&station=MEB&station=MQI&station=NCA&station=NKT&station=OAJ&station=PGV&station=POB&station=RDU&station=RWI&'

#CONUS: 32
metar_url_north_dakota =              'station=BIS&station=DIK&station=DVL&station=FAR&station=GFK&station=ISN&station=JMS&station=MIB&station=MOT&station=N60&station=RDR&'

#CONUS: 33
metar_url_ohio =                      'station=AKR&station=BKL&station=CAK&station=CGF&station=CLE&station=CMH&station=DAY&station=FDY&station=FFO&station=ILN&station=LCK&station=LNN&station=LUK&station=MFD&station=OSU&station=SGH&station=TDZ&station=TOL&station=YNG&station=ZZV&'

#CONUS: 34
metar_url_oklahama =                  'station=ADM&station=BVO&station=CLK&station=CSM&station=END&station=FSI&station=GAG&station=HBR&station=LTS&station=MKO&station=MLC&station=OKC&station=PNC&station=SWO&station=TIK&station=TUL&'

#CONUS: 35
metar_url_oregon =                    'station=AST&station=BKE&station=BNO&station=BOK&station=CVO&station=CZK&station=EUG&station=HIO&station=JNW&station=LGD&station=LMT&station=MEH&station=MFR&station=ONO&station=ONP&station=OTH&station=PDT&station=PDX&station=RBG&station=RDM&station=REO&station=SLE&station=SPB&station=SXT&station=TTD&'

#CONUS: 36
metar_url_pennsylvania =              'station=ABE&station=AGC&station=AOO&station=AVP&station=BFD&station=CXY&station=DUJ&station=ERI&station=FKL&station=IPT&station=JST&station=LBE&station=LNS&station=MDT&station=MUI&station=NXX&station=PHL&station=PIT&station=PNE&station=PSB&station=RDG&station=SEG&'

#CONUS: 37
metar_url_rhode_island =              'station=BID&station=OQU&station=PVD&'

#CONUS: 38
metar_url_south_carolina =            'station=AND&station=CAE&station=CHS&station=CRE&station=CUB&station=FLO&station=GMU&station=GSP&station=GYH&'

#CONUS: 39
metar_url_south_dakota =              'station=2WX&station=9V9&station=ABR&station=ATY&station=BKX&station=FSD&station=HON&station=LEM&station=MBG&station=MHE&station=PHP&station=PIR&station=RAP&station=RCA&station=Y22&station=YKN&'

#CONUS: 40
metar_url_tennessee =                 'station=BNA&station=CHA&station=CKV&station=CSV&station=DKX&station=DYR&station=MEM&station=MKL&station=MQY&station=MRC&station=NQA&station=TRI&station=TYS&station=UCY&'

#CONUS: 41
metar_url_texas =                     'station=ABI&station=ACT&station=ADS&station=ALI&station=AMA&station=ATT&station=BPT&station=BRO&station=BSM&station=BWD&station=CDS&station=CLL&station=CNW&station=COT&station=CRP&station=DAL&station=DFW&station=DHT&station=DLF&station=DRT&station=DYS&station=EFD&station=ELP&station=ERV&station=FTW&station=GDP&station=GGG&station=GLS&station=GRK&station=GVT&station=HDO&station=HLR&station=HOU&station=HRL&station=IAH&station=ILE&station=INK&station=JCT&station=LBB&station=LFK&station=LRD&station=MAF&station=MFE&station=MRF&station=MWL&station=NFW&station=NGP&station=NOG&station=NQI&station=PIL&station=PRX&station=PSX&station=PVW&station=RND&station=SAT&station=SJT&station=SPS&station=TPL&station=TYR&station=VCT&'

#CONUS: 42
metar_url_utah =                      'station=4BL&station=4HV&station=BCE&station=CDC&station=CNY&station=DPG&station=DTA&station=ENV&station=HIF&station=HVE&station=MLF&station=OGD&station=PUC&station=SLC&station=T62&station=U24&station=U28&station=VEL&'

#CONUS: 43
metar_url_vermont =                   'station=1V4&station=BTV&station=MPV&station=RUT&station=VSF&'

#CONUS: 44
metar_url_virginia =                  'station=CHO&station=CJR&station=DAA&station=DAN&station=DCA&station=FAF&station=FCI&station=HSP&station=IAD&station=LFI&station=LYH&station=MFV&station=NFE&station=NGU&station=NTU&station=NYG&station=OMH&station=ORF&station=PHF&station=PSK&station=RIC&station=ROA&station=SHD&'

#CONUS: 45
metar_url_washington =                'station=ALW&station=BFI&station=BLI&station=DEW&station=DLS&station=EAT&station=ELN&station=GEG&station=EPH&station=GRF&station=HMS&station=HQM&station=KLS&station=MWH&station=NOW&station=NUW&station=OKH&station=OLM&station=OMK&station=PAE&station=PSC&station=PUW&station=PWT&station=SEA&station=SFF&station=SHN&station=SKA&station=SMP&station=TCM&station=TDO&station=TIW&station=UIL&station=YKM&'

#CONUS: 46
metar_url_west_virginia =             'station=BKW&station=BLF&station=CKB&station=CRW&station=EKN&station=HLG&station=HTS&station=LWB&station=MGW&station=MRB&station=PKB&station=W99&'

#CONUS: 47
metar_url_wisconsin =                 'station=AIG&station=AUW&station=BUU&station=CMY&station=CWA&station=EAU&station=GRB&station=JVL&station=LNR&station=LSE&station=MKE&station=MSN&station=MTW&station=MWC&station=OSH&station=RHI&station=VOK&'

#CONUS: 48
metar_url_wyoming =                   'station=ARL&station=BPI&station=BRX&station=COD&station=CPR&station=CYS&station=EVW&station=FIR&station=GCC&station=JAC&station=LAR&station=LND&station=P60&station=RIW&station=RKS&station=RWL&station=SHR&station=WRL&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#UNITED STATES (OUTSIDE CONTIGUOUS)

#OCONUS: 1
metar_url_alaska =                    'station=PAGK&station=PAGM&station=PAGN&station=PAGS&station=PAGY&station=PAHN&station=PAHO&station=PAHV&station=PAHY&station=PAHZ&station=PAIL&station=PAIM&station=PAIN&station=PAJN&station=PAKF&station=PAKK&station=PAKN&station=PAKT&station=PALH&station=PALJ&station=PALK&station=PALR&station=PALU&station=PALV&station=PAMC&station=PAMD&station=PAMH&station=PAML&station=PAMR&station=PAMX&station=PAMY&station=PANC&station=PANI&station=PANN&station=PANT&station=PAOH&station=PAOM&station=PAPC&station=PAPG&station=PAPH&station=PAPM&station=PAPO&station=PAPR&station=PAPT&station=PARL&station=PARS&station=PARY&station=PASC&station=PASI&station=PASM&station=PASN&station=PASP&station=PASV&station=PASW&station=PASY&station=PATA&station=PATC&station=PATE&station=PATK&station=PATL&station=PAUM&station=PAUN&station=PAUO&station=PAVD&station=PAVW&station=PAWD&station=PAWG&station=PAWI&station=PAWR&station=PAWS&station=PAXK&station=PAYA&station=PFNO&station=PFYU&station=PPIZ&'

#OCONUS: 2
metar_url_hawaii =                    'station=PHBK&station=PHHI&station=PHHN&station=PHIK&station=PHJR&station=PHKO&station=PHLI&station=PHMK&station=PHNG&station=PHNL&station=PHNY&station=PHOG&station=PHSF&station=PHTO&station=PMDY&'

#OCONUS: 3
metar_url_puerto_rico =               'station=TJBQ&station=TJMZ&station=TJSJ&station=TJNR&'

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#COMBINE METARS URL

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CARIBB AND CAM

#COMBINES ALL "metar_url_[countries/territories]" IN CARIBBEAN AND CENTRAL AMERICA (INCLUDES PUERTO RICO)

metar_url_CARIBB_CAM_OCONUS =  [metar_url_place_holder, metar_url_antigua_barbuda, metar_url_belize, metar_url_british_virgin_islands, metar_url_costa_rica, metar_url_cuba, metar_url_dominica, metar_url_el_salvador, metar_url_guatemala, metar_url_haiti, metar_url_honduras, metar_url_jamaica, metar_url_nicaragua, metar_url_panama, metar_url_puerto_rico]
def join_elements_CARIBB_CAM_OCONUS(indices):
  selected_elements_CARIBB_CAM_OCONUS = [metar_url_CARIBB_CAM_OCONUS[i] for i in indices]
  return ''.join(selected_elements_CARIBB_CAM_OCONUS)

result_CARIBB_01_06_CAM_01_07_OCONUS_03 = join_elements_CARIBB_CAM_OCONUS(range(1,15))
metar_url_CARIBB_01_06_CAM_01_07_OCONUS_03 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_CARIBB_01_06_CAM_01_07_OCONUS_03}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_CARIBB_01_06_CAM_01_07_OCONUS_03 = requests.get(metar_url_CARIBB_01_06_CAM_01_07_OCONUS_03)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CAN AND GRL

#COMBINES ALL "metar_url_[provinces/territories]" IN CANADA (INCLUDES GREENLAND AND ALASKA)

metar_url_CAN_GRL_OCONUS =     [metar_url_place_holder, metar_url_alaska, metar_url_alberta, metar_url_british_columbia, metar_url_manitoba, metar_url_new_brunswick, metar_url_newfoundland, metar_url_northwest_territories, metar_url_nova_scotia, metar_url_nunavut, metar_url_ontario, metar_url_prince_edward_island, metar_url_quebec, metar_url_saskatchewan, metar_url_yukon, metar_url_greenland]
def join_elements_CAN_GRL_OCONUS(indices):
  selected_elements_CAN_GRL_OCONUS = [metar_url_CAN_GRL_OCONUS[i] for i in indices]
  return ''.join(selected_elements_CAN_GRL_OCONUS)

result_CAN_01_13_GRL_01_OCONUS_01 = join_elements_CAN_GRL_OCONUS(range(1,16))
metar_url_CAN_01_13_GRL_01_OCONUS_01 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_CAN_01_13_GRL_01_OCONUS_01}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_CAN_01_13_GRL_01_OCONUS_01 = requests.get(metar_url_CAN_01_13_GRL_01_OCONUS_01)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#MEX

#COMBINES ALL "metar_url_[state]" IN MEXICO

metar_url_MEX =             [metar_url_place_holder, metar_url_aguascalientes, metar_url_baja_california, metar_url_baja_california_sur, metar_url_campeche, metar_url_chiapas, metar_url_chihuahua, metar_url_coahuila, metar_url_colima, metar_url_durango, metar_url_guanajuato, metar_url_guerrero, metar_url_hidalgo, metar_url_jalisco, metar_url_mexico, metar_url_mexico_city, metar_url_michoacan, metar_url_morelos, metar_url_nayarit, metar_url_nuevo_leon, metar_url_oaxaca, metar_url_puebla, metar_url_queretaro, metar_url_quintana_roo, metar_url_san_luis_potosi, metar_url_sinaloa, metar_url_sonora, metar_url_tabasco, metar_url_tamaulipas, metar_url_tlaxcala, metar_url_veracruz, metar_url_yucatan, metar_url_zacatecas]
def join_elements_MEX(indices):
  selected_elements_MEX = [metar_url_MEX[i] for i in indices]
  return ''.join(selected_elements_MEX)

result_MEX_01_32 = join_elements_MEX(range(1,33))
metar_url_MEX_01_32 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_MEX_01_32}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_MEX_01_32 = requests.get(metar_url_MEX_01_32)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CONUS

#COMBINES ALL "metar_url_[state]"

metar_url_CONUS =           [metar_url_place_holder, metar_url_alabama, metar_url_arizona, metar_url_arterritory, metar_url_california, metar_url_colorado, metar_url_connecticut, metar_url_delaware, metar_url_florida, metar_url_georgia, metar_url_idaho, metar_url_illinois, metar_url_indiana, metar_url_iowa, metar_url_territory, metar_url_kentucky, metar_url_louisiana, metar_url_maine, metar_url_maryland, metar_url_massachusetts, metar_url_michigan, metar_url_minnesota, metar_url_mississippi, metar_url_missouri, metar_url_montana, metar_url_nebraska, metar_url_nevada, metar_url_new_hampshire, metar_url_new_mexico, metar_url_new_jersey, metar_url_new_york, metar_url_north_carolina, metar_url_north_dakota, metar_url_ohio, metar_url_oklahama, metar_url_oregon, metar_url_pennsylvania, metar_url_rhode_island, metar_url_south_carolina, metar_url_south_dakota, metar_url_tennessee, metar_url_texas, metar_url_utah, metar_url_vermont, metar_url_virginia, metar_url_washington, metar_url_west_virginia, metar_url_wisconsin, metar_url_wyoming]
def join_elements_CONUS(indices):
  selected_elements_CONUS = [metar_url_CONUS[i] for i in indices]
  return ''.join(selected_elements_CONUS)

#CONUS STATES 01 THROUGH 24

result_CONUS_01_24 = join_elements_CONUS(range(1,25))
metar_url_CONUS_01_24 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_CONUS_01_24}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_CONUS_01_24 = requests.get(metar_url_CONUS_01_24)

#CONUS STATES 25 THROUGH 48

result_CONUS_25_48 = join_elements_CONUS(range(25,49))
metar_url_CONUS_25_48 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_CONUS_25_48}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_CONUS_25_48 = requests.get(metar_url_CONUS_25_48)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CARIBB, CAM, OCONUS

metar_data_CARIBB_01_06_CAM_01_07_OCONUS_03 = pd.read_csv(io.StringIO(surface_data_CARIBB_01_06_CAM_01_07_OCONUS_03.text))

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CAN, GRL, OCONUS

metar_data_CAN_01_13_GRL_01_OCONUS_01 = pd.read_csv(io.StringIO(surface_data_CAN_01_13_GRL_01_OCONUS_01.text))

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#MEX

metar_data_MEX_01_32 = pd.read_csv(io.StringIO(surface_data_MEX_01_32.text))

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#CONUS

metar_data_CONUS_01_24 = pd.read_csv(io.StringIO(surface_data_CONUS_01_24.text))
metar_data_CONUS_25_48 = pd.read_csv(io.StringIO(surface_data_CONUS_25_48.text))

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#COMBINES ALL NORTH AMERICAN METAR STATIONS

metar_data_NORTH_AMERICA = pd.concat([metar_data_CARIBB_01_06_CAM_01_07_OCONUS_03, metar_data_CAN_01_13_GRL_01_OCONUS_01, metar_data_MEX_01_32, metar_data_CONUS_01_24, metar_data_CONUS_25_48])


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#MISSOURI STATIONS


metar_url_MIZ =             [metar_url_place_holder, metar_url_missouri]
def join_elements_MIZ(indices):
  selected_elements_MIZ = [metar_url_MIZ[i] for i in indices]
  return ''.join(selected_elements_MIZ)

result_MIZ_01_02 = join_elements_MIZ(range(1,2))
metar_url_MIZ_01_02 = f'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{result_MIZ_01_02}data=all&year1={year1}&month1={month1}&day1={day1}&year2={year2}&month2={month2}&day2={day2}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&elev=yes&missing=null&trace=T&direct=no&report_type=3&report_type=4'
surface_data_MIZ_01_02 = requests.get(metar_url_MIZ_01_02)

#MIZ

metar_data_MIZ_01_02 = pd.read_csv(io.StringIO(surface_data_MIZ_01_02.text))

#metar_data_MIZ = pd.concat(metar_data_MIZ_01_02)



#IMPORTANT, THIS IS WHAT BUILDS OUR SHIT

ncss = metar_data_MIZ_01_02

from datetime import datetime
import re
wind_spd_data = []
wind_dir_data = []
temp_data = []
DWP_data = []
SLP_data = []
#temp_dwp_pattern = r' (?i)(M?[0-9]{2}?)/(M?[0-9]{2}?)( A[0-9]{4}?) '
wind_spd_pattern = r'(VRB|[0-9]{3})?([0-9]{2})?(G[0-9]{1,3})?KT'
SLP_pattern = r'(?i)SLP([0-9]{3})'
Surface_Data = surface_data_CARIBB_01_06_CAM_01_07_OCONUS_03.text + surface_data_CAN_01_13_GRL_01_OCONUS_01.text + surface_data_MEX_01_32.text + surface_data_CONUS_01_24.text + surface_data_CONUS_25_48.text
with open('Surface_Data.text', 'w') as f:
  f.write(Surface_Data)
Text_Data = Surface_Data
for line in Text_Data.splitlines():
  wind_spd_match = re.search(wind_spd_pattern, line)
  #temp_dwp_match = re.search(temp_dwp_pattern, line)
  SLP_match = re.search(SLP_pattern, line)
  wind_spd = wind_spd_match.group(2) if wind_spd_match else None
  wind_dir = wind_spd_match.group(1) if wind_spd_match else None
  #temp = temp_dwp_match.group(1) if temp_dwp_match else None
  #DWP = temp_dwp_match.group(2) if temp_dwp_match else None
  SLP = SLP_match.group(1) if SLP_match else None
  wind_spd_data.append(wind_spd)
  wind_dir_data.append(wind_dir)
  #temp_data.append(temp)
  #DWP_data.append(DWP)
  SLP_data.append(SLP)
wind_spd_data.pop(0)
wind_dir_data.pop(0)
#temp_data.pop(0)
#DWP_data.pop(0)
SLP_data.pop(0)
Wind_Speed = pd.Series(wind_spd_data, name = 'Wind_Speed')
Wind_Direction = pd.Series(wind_dir_data, name = 'Wind_Direction')
#Temperature = pd.Series(temp_data, name = 'Temperature')
#Dew_Point = pd.Series(DWP_data, name = 'Dew_Point')
Sea_Level_Pressure = pd.Series(SLP_data, name = 'Pressure')

ncss = ncss.reset_index(drop=True)
ncss = pd.concat([ncss, Sea_Level_Pressure], axis=1)

months = {1: "JANUARY", 2: "FEBRUARY", 3: "MARCH", 4: "APRIL", 5: "MAY", 6: "JUNE", 7: "JULY", 8: "AUGUST", 9: "SEPTEMBER", 10: "OCTOBER", 11: "NOVEMBER", 12: "DECEMBER"}

if month1 == 1:
    month = months[month1]
elif month1 == 2:
    month = months[month1]
elif month1 == 3:
    month = months[month1]
elif month1 == 4:
    month = months[month1]
elif month1 == 5:
    month = months[month1]
elif month1 == 6:
    month = months[month1]
elif month1 == 7:
    month = months[month1]
elif month1 == 8:
    month = months[month1]
elif month1 == 9:
    month = months[month1]
elif month1 == 10:
    month = months[month1]
elif month1 == 11:
    month = months[month1]
elif month1 == 12:
    month = months[month1]

#print("DATE RANGE FOR LOADED SURFACE DATA:")
#print("-----------------------------------")
#print("YEAR:  ", year1)
#print("MONTH: ", month)
#print("DAYS:  ", day1, "-", day2)

from datetime import datetime

#SURFACE MAP DATE AND TIME (yyyy, m/mm, d/dd, hh)
year  = 2025
month = 11
day   = 29
hour  = 18

date = datetime(year, month, day, hour, 00)
ncss['valid'] = pd.to_datetime(ncss['valid'])
filtered_data = ncss[(ncss['valid']==date)]



import numpy as np
import metpy.calc as mpcalc
from metpy.units import units

filtered_data['tmpf'] = pd.to_numeric(filtered_data['tmpf'], errors='coerce')
filtered_data['dwpf'] = pd.to_numeric(filtered_data['dwpf'], errors='coerce')
filtered_data['tmpf'].fillna(0, inplace=True)
filtered_data['dwpf'].fillna(0, inplace=True)
filtered_data['tmpf'] = filtered_data['tmpf'].astype(float)
filtered_data['dwpf'] = filtered_data['dwpf'].astype(float)

filtered_data['Pressure'] = filtered_data['Pressure'].astype(str)
filtered_data['Pressure'] = filtered_data['Pressure'].replace('None', '0')

lats = filtered_data['lat']
lons = filtered_data['lon']
lats = lats.astype(float)
lons = lons.astype(float)
tair = filtered_data['tmpf']
dewpt = filtered_data['dwpf']
pressure = filtered_data['Pressure']

filtered_data['sknt'] = pd.to_numeric(filtered_data['sknt'], errors='coerce')
filtered_data['drct'] = pd.to_numeric(filtered_data['drct'], errors='coerce')
filtered_data['sknt'].fillna(0, inplace=True)
filtered_data['drct'].fillna(0, inplace=True)
filtered_data['sknt'] = filtered_data['sknt'].astype(int)
filtered_data['drct'] = filtered_data['drct'].astype(int)

wind_speed_array = np.array(filtered_data['sknt'])
wind_direction_array = np.array(filtered_data['drct'])
u, v = mpcalc.wind_components(wind_speed_array*units.knots, wind_direction_array*units.degrees)
cloud_cover = []
cloud_cover = np.pad(cloud_cover, (0, len(filtered_data)-len(cloud_cover)), 'constant', constant_values=10)
cloud_cover = cloud_cover.astype(int)
stid = np.array(filtered_data['station'].astype(str))
tair = np.array(tair.astype(float))
dewpt = np.array(dewpt.astype(float))
lats = np.array(lats.astype(float))
lons = np.array(lons.astype(float))
u = np.array(u.astype(float))
v = np.array(v.astype(float))
pressure = np.array(pressure.astype(str))

lats = np.nan_to_num(lats)
lons = np.nan_to_num(lons)
tair = np.nan_to_num(tair)
dewpt = np.nan_to_num(dewpt)
u = np.nan_to_num(u)
v = np.nan_to_num(v)
pressure = np.nan_to_num(pressure)

mask = (lats != 0) & (lons != 0)
lats = lats[mask]
lons = lons[mask]
tair = tair[mask]
dewpt = dewpt[mask]
u = u[mask]
v = v[mask]
cloud_cover = cloud_cover[mask]
stid = stid[mask]
pressure = pressure[mask]

u = np.around(u, decimals=5)
v = np.around(v, decimals=5)



# MAP CREATOR!

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
from metpy.plots import StationPlot, sky_cover
fig = plt.figure(figsize=(150,210))
proj = ccrs.NorthPolarStereo(central_longitude=-92.5)
#proj = ccrs.NorthPolarStereo(central_longitude=-105)
ax = fig.add_subplot(1,1,1, projection=proj)

#adds physical map features
ax.add_feature(cfeature.OCEAN)
land_10m = cfeature.NaturalEarthFeature('physical', 'land', '10m')
ax.add_feature(land_10m, edgecolor='black', facecolor=cfeature.COLORS['land'])
ax.add_feature(cfeature.LAKES, alpha=0.75)
ax.coastlines(resolution='10m')

#human map features
#ax.add_feature(cfeature.BORDERS)
ax.gridlines()

#ADDS BORDERS TO STATES AND PROVINCES
provinces = cfeature.NaturalEarthFeature(
    category='cultural',
    name='admin_1_states_provinces_lines',
    scale='10m',
    facecolor='none',
    edgecolor='black'
)
ax.add_feature(provinces)

ax.scatter(lons, lats, c=cloud_cover, cmap='Reds', transform=ccrs.PlateCarree(), zorder=10)
ax.barbs(lons, lats, u, v, transform=ccrs.PlateCarree(), length = 10, linewidth= 1.4, zorder=10)
for i in range(len(lons)):
  ax.text(lons[i]-0.15, lats[i]+.05, f'{tair[i]}', fontsize=12, color='red', transform=ccrs.PlateCarree(), ha='right', va='bottom')
  ax.text(lons[i]-0.15, lats[i]-0.05, f'{dewpt[i]:.0f}', fontsize = 12, color='green', transform=ccrs.PlateCarree(), ha='right', va='top')
  ax.text(lons[i]+0.15, lats[i]+0.05, f'{pressure[i]}', fontsize=12, color='blue', transform=ccrs.PlateCarree(), ha='left', va='bottom')
  ax.text(lons[i]+0.15, lats[i]-0.05, f'{stid[i]}', fontsize=12, color='black', transform=ccrs.PlateCarree(), ha='left', va='top')
#ax.set_extent([-143, -60, 17, 83], crs=ccrs.PlateCarree())
ax.set_extent([-95.525, -89.05, 35.925, 40.775], crs=ccrs.PlateCarree())
'''
stationplot = StationPlot(ax, lons, lats, transform=ccrs.PlateCarree(), fontsize=12)
stationplot.plot_parameter('NW', tair, color='red')
stationplot.plot_barb(u, v)
stationplot.plot_symbol('C', cloud_cover, sky_cover)
'''
plt.show()