import { useState } from 'react';
import { Alert, AlertDescription, AlertTitle } from './components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card';
import QueryEditor from './components/query/QueryEditor';
import ResultsTable from './components/results/ResultsTable';
import './App.css';

function App() {
  const [results, setResults] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string>('query');
  
  const tableLocation = 'my-table';
  
  const handleQueryResults = (newResults: any[], newColumns: string[]) => {
    setResults(newResults);
    setColumns(newColumns);
    setError(null);
    setActiveTab('results');
  };
  
  const handleError = (errorMessage: string) => {
    setError(errorMessage);
    setActiveTab('query');
  };
  
  const clearError = () => {
    setError(null);
  };

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      {/* Header */}
      <header className="bg-white shadow-sm py-4 px-6 border-b">
        <div className="container mx-auto flex justify-between items-center">
          <h1 className="text-2xl font-bold text-gray-900">Iceberg DuckDB Explorer</h1>
          <div className="text-sm text-gray-500">
            Table: {tableLocation}
          </div>
        </div>
      </header>
      
      {/* Main Content */}
      <main className="flex-1 container mx-auto py-6 px-4">
        {/* Error Alert */}
        {error && (
          <Alert variant="destructive" className="mb-6">
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>
              {error}
              <button 
                onClick={clearError}
                className="ml-2 underline"
              >
                Dismiss
              </button>
            </AlertDescription>
          </Alert>
        )}
        
        {/* Tabs for Query and Results */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full grid-cols-2 mb-6">
            <TabsTrigger value="query">Query Editor</TabsTrigger>
            <TabsTrigger value="results" disabled={results.length === 0}>
              Results {results.length > 0 ? `(${results.length})` : ''}
            </TabsTrigger>
          </TabsList>
          
          <TabsContent value="query">
            <Card>
              <CardHeader>
                <CardTitle>SQL Query</CardTitle>
                <CardDescription>
                  Write SQL queries to explore Iceberg data using DuckDB-WASM
                </CardDescription>
              </CardHeader>
              <CardContent>
                <QueryEditor 
                  onQueryResults={handleQueryResults} 
                  onError={handleError}
                  tableLocation={tableLocation}
                />
              </CardContent>
            </Card>
          </TabsContent>
          
          <TabsContent value="results">
            <Card>
              <CardHeader>
                <CardTitle>Query Results</CardTitle>
                <CardDescription>
                  Displaying {results.length} rows from Iceberg data
                </CardDescription>
              </CardHeader>
              <CardContent>
                <ResultsTable 
                  results={results} 
                  columns={columns} 
                />
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </main>
      
      {/* Footer */}
      <footer className="bg-white shadow-sm py-4 px-6 border-t mt-auto">
        <div className="container mx-auto text-center text-sm text-gray-500">
          Iceberg DuckDB Explorer - Powered by DuckDB-WASM
        </div>
      </footer>
    </div>
  );
}

export default App;
