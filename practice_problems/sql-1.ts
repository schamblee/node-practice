import { Pool, QueryResult } from 'pg';

/**
 * Represents a wastewater data record from the database
 */
interface WastewaterRecord {
  id: number;
  site_id: string;
  site_name: string;
  collection_date: string; // ISO format date (YYYY-MM-DD)
  virus_concentration: number;
  population_served: number;
  city: string;
  state: string;
  zipcode: string;
  flow_rate: number;
  precipitation: number;
  temperature: number;
  ph_level: number;
}

/**
 * Filter options for querying wastewater data
 */
interface FilterOptions {
  startDate: string; // ISO format date (YYYY-MM-DD)
  endDate: string; // ISO format date (YYYY-MM-DD)
  location?: {
    city?: string;
    state?: string;
    siteId?: string;
    zipCode?: string;
  };
  limit?: number;
  offset?: number;
}

/**
 * Response format for the filtered data
 */
interface FilteredDataResponse {
  data: WastewaterRecord[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
  metadata: {
    dateRange: {
      start: string;
      end: string;
    };
    locations: {
      cities: string[];
      states: string[];
      siteCount: number;
    };
  };
}

export async function fetchWastewaterData(
  pool: Pool,
  options: FilterOptions
): Promise<FilteredDataResponse> {
  try {
    const {
      startDate,
      endDate,
      location,
      limit = 100,
      offset = 0
    } = options;

    const params: any[] = [startDate, endDate];
    let whereClause = 'collection_date BETWEEN $1 AND $2';
    let paramIndex = 3;

    if (location) {
      if (location.city) {
        whereClause += ` AND city = $${paramIndex}`;
        params.push(location.city);
        paramIndex++;
      }
      if (location.state) {
        whereClause += ` AND state = $${paramIndex}`;
        params.push(location.state);
        paramIndex++;
      }
      if (location.siteId) {
        whereClause += ` AND site_id = $${paramIndex}`;
        params.push(location.siteId);
        paramIndex++;
      }
      if (location.zipCode) {
        whereClause += ` AND zipcode = $${paramIndex}`;
        params.push(location.zipCode);
        paramIndex++;
      }
    }

    const countQuery = `
      SELECT COUNT(*) as count 
      FROM wastewater_data
      WHERE ${whereClause}
    `;
    
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);
    
    const dataQuery = `
      SELECT * FROM wastewater_data
      WHERE ${whereClause}
      ORDER BY collection_date, site_id
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    
    const dataParams = [...params, limit, offset];
    const dataResult = await pool.query(dataQuery, dataParams);
    const records = dataResult?.rows || [];

    const cities = Array.from(new Set(records.map(r => r.city)));
    const states = Array.from(new Set(records.map(r => r.state)));
    const uniqueSites = new Set(records.map(r => r.site_id));
    
    return {
      data: records,
      pagination: {
        total,
        limit,
        offset,
        hasMore: (offset + limit < total)
      },
      metadata: {
        dateRange: {
          start: startDate,
          end: endDate
        },
        locations: {
          cities,
          states,
          siteCount: uniqueSites.size
        }
      }
    };
  } catch (error) {
    throw error;
  }
}
