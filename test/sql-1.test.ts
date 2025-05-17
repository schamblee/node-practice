import { Pool, QueryResult } from 'pg';
import { fetchWastewaterData } from '../practice_problems/sql-1';

// Mock the pg Pool
jest.mock('pg', () => {
  const mockPool = {
    query: jest.fn(),
    end: jest.fn(),
    on: jest.fn(),
  };
  return { Pool: jest.fn(() => mockPool) };
});

describe('Wastewater Data Fetching', () => {
  let pool: jest.Mocked<Pool>;
  
  beforeEach(() => {
    pool = new Pool() as any;
    jest.clearAllMocks();
  });
  
  // Sample data that would be returned from the database
  const mockDbResponse = {
    rows: [
      {
        id: 1,
        site_id: 'SITE001',
        site_name: 'Downtown Treatment Facility',
        collection_date: '2025-05-10',
        virus_concentration: 350.5,
        population_served: 125000,
        city: 'Boston',
        state: 'MA',
        zipcode: '02108',
        flow_rate: 24500000,
        precipitation: 0.2,
        temperature: 65.3,
        ph_level: 7.2
      },
      {
        id: 2,
        site_id: 'SITE001',
        site_name: 'Downtown Treatment Facility',
        collection_date: '2025-05-11',
        virus_concentration: 342.8,
        population_served: 125000,
        city: 'Boston',
        state: 'MA',
        zipcode: '02108',
        flow_rate: 24100000,
        precipitation: 0.0,
        temperature: 67.1,
        ph_level: 7.3
      },
      {
        id: 3,
        site_id: 'SITE002',
        site_name: 'Riverside Collection Point',
        collection_date: '2025-05-10',
        virus_concentration: 425.1,
        population_served: 78000,
        city: 'Cambridge',
        state: 'MA',
        zipcode: '02142',
        flow_rate: 15600000,
        precipitation: 0.2,
        temperature: 65.7,
        ph_level: 7.1
      }
    ],
    rowCount: 3
  };
  
  // Mock count query response
  const mockCountResponse = {
    rows: [{ count: '3' }],
    rowCount: 1
  };

  it('should fetch data with date range filter', async () => {
    // Mock the query responses
    pool.query
      // @ts-ignore
      .mockResolvedValueOnce(mockCountResponse as QueryResult)
      // @ts-ignore
      .mockResolvedValueOnce(mockDbResponse as QueryResult);;
    
    const result = await fetchWastewaterData(pool, {
      startDate: '2025-05-10',
      endDate: '2025-05-15'
    });
    
    // Verify the correct queries were made
    expect(pool.query).toHaveBeenCalledTimes(2);
    
    // First call should be the COUNT query
    expect(pool.query.mock.calls[0][0]).toContain('COUNT');
    expect(pool.query.mock.calls[0][0]).toContain('collection_date BETWEEN');
    
    // Second call should be the data query
    expect(pool.query.mock.calls[1][0]).toContain('SELECT');
    expect(pool.query.mock.calls[1][0]).toContain('collection_date BETWEEN');
    
    // Check the parameters
    expect(pool.query.mock.calls[0][1]).toContain('2025-05-10');
    expect(pool.query.mock.calls[0][1]).toContain('2025-05-15');
    
    // Verify response structure
    expect(result.data.length).toBe(3);
    expect(result.pagination.total).toBe(3);
    expect(result.pagination.hasMore).toBe(false);
    expect(result.metadata.locations.cities).toContain('Boston');
    expect(result.metadata.locations.cities).toContain('Cambridge');
    expect(result.metadata.locations.states).toContain('MA');
    expect(result.metadata.locations.siteCount).toBe(2); // Two unique sites in data
  });

  it('should filter by location', async () => {
    // Filter for just Boston
    const bostonData = {
      rows: mockDbResponse.rows.filter(row => row.city === 'Boston'),
      rowCount: 2
    };
    
    const bostonCount = {
      rows: [{ count: '2' }],
      rowCount: 1
    };
    
    pool.query
      // @ts-ignore
      .mockResolvedValueOnce(bostonCount as QueryResult)
      // @ts-ignore
      .mockResolvedValueOnce(bostonData as QueryResult);    
    const result = await fetchWastewaterData(pool, {
      startDate: '2025-05-10',
      endDate: '2025-05-15',
      location: {
        city: 'Boston'
      }
    });
    
    // Check SQL contains city filter
    expect(pool.query.mock.calls[0][0]).toContain('city =');
    expect(pool.query.mock.calls[0][1]).toContain('Boston');
    
    // Verify response contains only Boston data
    expect(result.data.length).toBe(2);
    expect(result.pagination.total).toBe(2);
    expect(result.metadata.locations.cities).toEqual(['Boston']);
    expect(result.metadata.locations.siteCount).toBe(1);
  });

  it('should handle pagination correctly', async () => {
    // Mock responses for pagination test
    // @ts-ignore
    pool.query.mockResolvedValueOnce(mockCountResponse as QueryResult).mockResolvedValueOnce({
        rows: [mockDbResponse.rows[0]], // Just the first record
        rowCount: 1
      } as QueryResult);
    
    const result = await fetchWastewaterData(pool, {
      startDate: '2025-05-10',
      endDate: '2025-05-15',
      limit: 1,
      offset: 0
    });
    
    // Verify pagination parameters in SQL
    expect(pool.query.mock.calls[1][0]).toContain('LIMIT');
    expect(pool.query.mock.calls[1][0]).toContain('OFFSET');
    
    // Check pagination metadata
    expect(result.data.length).toBe(1);
    expect(result.pagination.total).toBe(3); // Total count from mock
    expect(result.pagination.limit).toBe(1);
    expect(result.pagination.offset).toBe(0);
    expect(result.pagination.hasMore).toBe(true); // More data available
  });

  it('should handle empty results', async () => {
    // Mock empty results
    // @ts-ignore
    pool.query.mockResolvedValueOnce({ rows: [{ count: '0' }], rowCount: 1 } as QueryResult).mockResolvedValueOnce({ rows: [], rowCount: 0 } as QueryResult);
    
    const result = await fetchWastewaterData(pool, {
      startDate: '2026-01-01', // Future date with no data
      endDate: '2026-01-31'
    });
    
    // Verify empty response structure
    expect(result.data).toEqual([]);
    expect(result.pagination.total).toBe(0);
    expect(result.pagination.hasMore).toBe(false);
    expect(result.metadata.locations.cities).toEqual([]);
    expect(result.metadata.locations.states).toEqual([]);
    expect(result.metadata.locations.siteCount).toBe(0);
  });

  it('should handle database errors gracefully', async () => {
    // Mock a database error
    // @ts-ignore
    pool.query.mockRejectedValueOnce(new Error('Database connection failed'));
    
    // Check that the function correctly propagates the error
    await expect(fetchWastewaterData(pool, {
      startDate: '2025-05-10',
      endDate: '2025-05-15'
    })).rejects.toThrow('Database connection failed');
  });
});
