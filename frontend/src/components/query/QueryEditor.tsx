import { useState } from 'react';
import { Textarea } from '../ui/textarea';
import { Button } from '../ui/button';
import { Fab } from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import duckDBService from '../../services/duckdb/DuckDBService';
import { Backdrop, CircularProgress } from '@mui/material';

interface QueryEditorProps {
  onQueryResults: (results: any[], columns: string[]) => void;
  onError: (error: string) => void;
  tableLocation: string;
}

const QueryEditor = ({ onQueryResults, onError, tableLocation }: QueryEditorProps) => {
  const [query, setQuery] = useState<string>('SELECT * FROM iceberg_data LIMIT 100;');
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isInitialized, setIsInitialized] = useState<boolean>(false);

  const executeQuery = async () => {
    if (!query.trim()) {
      onError('Query cannot be empty');
      return;
    }

    setIsLoading(true);
    try {
      if (!isInitialized) {
        await duckDBService.initialize();
        await duckDBService.setupIcebergTable(tableLocation);
        setIsInitialized(true);
      }

      const results = await duckDBService.executeQuery(query);
      
      const columns = results.length > 0 
        ? Object.keys(results[0]) 
        : [];
      
      onQueryResults(results, columns);
    } catch (error: any) {
      console.error('Error executing query:', error);
      onError(error.message || 'An error occurred while executing the query');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="space-y-4 p-4 bg-white rounded-lg shadow-md">
      <h2 className="text-xl font-bold text-gray-800">SQL Query Editor</h2>
      
      <Textarea
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Enter SQL query..."
        className="min-h-[200px] font-mono text-sm"
      />
      
      <div className="flex justify-end">
        <Button 
          variant="outline" 
          className="mr-2"
          onClick={() => setQuery('SELECT * FROM iceberg_data LIMIT 100;')}
        >
          Reset
        </Button>
        
        {/* Material-UI Floating Action Button for primary action */}
        <Fab 
          color="primary" 
          onClick={executeQuery}
          disabled={isLoading}
          aria-label="execute query"
        >
          <PlayArrowIcon />
        </Fab>
      </div>
      
      {/* Backdrop for visual feedback during state changes */}
      <Backdrop
        sx={{ color: '#fff', zIndex: (theme) => theme.zIndex.drawer + 1 }}
        open={isLoading}
      >
        <CircularProgress color="inherit" />
      </Backdrop>
    </div>
  );
};

export default QueryEditor;
