/**
 * Represents a raw wastewater measurement from a collection site
 */
interface RawMeasurement {
    siteId: string;
    siteName: string;
    city: string;
    state: string;
    sampleDate: string; // ISO format date string (YYYY-MM-DD)
    collectionTime: string; // Time in HH:MM format
    virusConcentration: number; // Virus concentration in copies/mL
    populationServed: number; // Population count for the collection site
    flowRate: number; // Flow rate in gallons per day
    precipitationLevel: number; // Precipitation in inches
  }
  
  /**
   * Represents aggregated measurement data by location and date
   */
  interface AggregatedResult {
    locationId: string;
    locationName: string;
    region: {
      city: string;
      state: string;
    };
    date: string; // ISO format date (YYYY-MM-DD)
    measurements: {
      averageVirusConcentration: number;
      totalSamples: number;
      normalizedConcentration: number; // virusConcentration adjusted for population
      flowRateAverage: number;
      precipitationAverage: number;
    };
  }
  
  /**
   * Transforms an array of raw measurements into aggregated results by location and date.
   * 
   * Requirements:
   * 1. Group measurements by siteId and sampleDate
   * 2. Calculate averages for virus concentration, flow rate, and precipitation
   * 3. Count the number of samples for each site/date combination
   * 4. Calculate a normalized concentration (virusConcentration / populationServed * 100000)
   * 5. Return the results in a structured format as defined by AggregatedResult
   * 
   * @param measurements - Array of raw measurement objects from collection sites
   * @returns Array of aggregated results by location and date
   */
  export function aggregateMeasurementsByLocationAndDate(
    measurements: RawMeasurement[]
  ): AggregatedResult[] {

    const grouped = measurements.reduce((acc, measurement) => {
      const key = `${measurement.siteId}_${measurement.sampleDate}`;

      if (!acc[key]) {
        acc[key] = [];
      }
      
      acc[key].push(measurement);

      return acc;
    }, {} as Record<string, RawMeasurement[]>);

    return Object.values(grouped).map((measurements) => {
      const { siteId: locationId, siteName: locationName, city, state, sampleDate: date, populationServed } = measurements[0];
      const totalSamples = measurements.length;
      const totalConcentration = measurements.reduce((total, next) => total + next.virusConcentration, 0);
      const averageVirusConcentration = totalConcentration / totalSamples;
      const normalizedConcentration = Math.round(averageVirusConcentration / populationServed * 10000000) / 100;
      const flowRateAverage = measurements.reduce((total, next) => total + next.flowRate, 0) / totalSamples;
      const precipitationAverage = measurements.reduce((total, next) => total + next.precipitationLevel, 0) / totalSamples;


      return {
        locationId,
        locationName,
        region: {
          city,
          state
        },
        date,
        measurements: {
          averageVirusConcentration,
          totalSamples,
          normalizedConcentration,
          flowRateAverage,
          precipitationAverage
        }
      }
    })
  }
  