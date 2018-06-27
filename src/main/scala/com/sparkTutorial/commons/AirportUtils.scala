package com.sparkTutorial.commons

/**
  * Created by kneupane on 6/27/18.
  */
object AirportUtils {

  /**
    * Each row of the input file contains the following columns:
    * sAirport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
    */
  case class AirPort(
                      airport_id: String,
                      name_of_airport: String,
                      main_city_served_by_airport: String,
                      country_where_airport_is_located: String,
                      iata_faa_code: String,
                      icao_code: String,
                      latitude: String,
                      longitude: String,
                      altitude: String,
                      timezone: String,
                      dst: String,
                      timezone_in_olson_format: String
                    )


  def parseEachLine(lines: Iterator[String]): Iterator[AirPort] = {
    lines.map(lines => {
      var line = lines.split(Utils.COMMA_DELIMITER)
      AirPort.apply(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11))
    })
  }


}
