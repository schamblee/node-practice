import { aggregateMeasurementsByLocationAndDate } from '../practice_problems/data-transformation-1';

describe('Data Aggregation', () => {
  // Sample test data
  const rawMeasurements = [
    {
      siteId: 'SITE001',
      siteName: 'Downtown Treatment Facility',
      city: 'Boston',
      state: 'MA',
      sampleDate: '2025-05-01',
      collectionTime: '09:30',
      virusConcentration: 350.5,
      populationServed: 125000,
      flowRate: 24500000,
      precipitationLevel: 0.2
    },
    {
      siteId: 'SITE001',
      siteName: 'Downtown Treatment Facility',
      city: 'Boston',
      state: 'MA',
      sampleDate: '2025-05-01',
      collectionTime: '15:45',
      virusConcentration: 375.2,
      populationServed: 125000,
      flowRate: 25100000,
      precipitationLevel: 0.3
    },
    {
      siteId: 'SITE001',
      siteName: 'Downtown Treatment Facility',
      city: 'Boston',
      state: 'MA',
      sampleDate: '2025-05-02',
      collectionTime: '10:15',
      virusConcentration: 310.8,
      populationServed: 125000,
      flowRate: 23800000,
      precipitationLevel: 0.0
    },
    {
      siteId: 'SITE002',
      siteName: 'Riverside Collection Point',
      city: 'Cambridge',
      state: 'MA',
      sampleDate: '2025-05-01',
      collectionTime: '08:45',
      virusConcentration: 425.1,
      populationServed: 78000,
      flowRate: 15600000,
      precipitationLevel: 0.2
    },
    {
      siteId: 'SITE002',
      siteName: 'Riverside Collection Point',
      city: 'Cambridge',
      state: 'MA',
      sampleDate: '2025-05-02',
      collectionTime: '09:00',
      virusConcentration: 390.3,
      populationServed: 78000,
      flowRate: 15200000,
      precipitationLevel: 0.1
    }
  ];

  it('should aggregate measurements by location and date', () => {
    const result = aggregateMeasurementsByLocationAndDate(rawMeasurements);
    
    // Expected results after aggregation
    expect(result.length).toBe(4); // 3 unique site/date combinations
    
    // Check the first aggregated result (SITE001, 2025-05-01)
    const site1Day1 = result.find(r => r.locationId === 'SITE001' && r.date === '2025-05-01');
    expect(site1Day1).toBeDefined();
    expect(site1Day1?.locationName).toBe('Downtown Treatment Facility');
    expect(site1Day1?.region.city).toBe('Boston');
    expect(site1Day1?.region.state).toBe('MA');
    expect(site1Day1?.measurements.totalSamples).toBe(2);
    expect(site1Day1?.measurements.averageVirusConcentration).toBeCloseTo(362.85, 2); // (350.5 + 375.2) / 2
    expect(site1Day1?.measurements.normalizedConcentration).toBeCloseTo(290.28, 2); // 362.85 / 125000 * 100000
    expect(site1Day1?.measurements.flowRateAverage).toBeCloseTo(24800000, 2); // (24500000 + 25100000) / 2
    expect(site1Day1?.measurements.precipitationAverage).toBeCloseTo(0.25, 2); // (0.2 + 0.3) / 2
    
    // Check the second aggregated result (SITE001, 2025-05-02)
    const site1Day2 = result.find(r => r.locationId === 'SITE001' && r.date === '2025-05-02');
    expect(site1Day2).toBeDefined();
    expect(site1Day2?.measurements.totalSamples).toBe(1);
    expect(site1Day2?.measurements.averageVirusConcentration).toBeCloseTo(310.8, 2);
    
    // Check the third aggregated result (SITE002, 2025-05-01)
    const site2Day1 = result.find(r => r.locationId === 'SITE002' && r.date === '2025-05-01');
    expect(site2Day1).toBeDefined();
    expect(site2Day1?.measurements.totalSamples).toBe(1);
    expect(site2Day1?.measurements.normalizedConcentration).toBeCloseTo(545, 2); // 425.1 / 78000 * 100000
  });

  it('should handle empty input array', () => {
    const result = aggregateMeasurementsByLocationAndDate([]);
    expect(result).toEqual([]);
  });

  it('should correctly process a single measurement', () => {
    const singleMeasurement = [rawMeasurements[0]];
    const result = aggregateMeasurementsByLocationAndDate(singleMeasurement);
    
    expect(result.length).toBe(1);
    expect(result[0].measurements.totalSamples).toBe(1);
    expect(result[0].measurements.averageVirusConcentration).toBe(350.5);
  });
});
