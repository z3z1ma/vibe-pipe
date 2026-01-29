import { useState, useEffect } from 'react';
import { useAuth } from '../hooks/useAuth';
import { api } from '../services/api';
import type { Pipeline, Asset } from '../types/api';

export function DashboardPage() {
  const { user, logout } = useAuth();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      const [pipelinesData, assetsData] = await Promise.all([
        api.getPipelines(),
        api.getAssets(),
      ]);

      setPipelines(pipelinesData.pipelines);
      setAssets(assetsData.assets);
    } catch (err) {
      setError('Failed to load data');
      console.error('Load data error:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleRunPipeline = async (pipelineId: string) => {
    try {
      await api.runPipeline(pipelineId);
      alert('Pipeline run triggered successfully!');
    } catch (err) {
      alert('Failed to run pipeline');
      console.error('Run pipeline error:', err);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-gray-600">Loading...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8 flex justify-between items-center">
          <h1 className="text-2xl font-bold text-gray-900">Vibe Piper</h1>
          <div className="flex items-center space-x-4">
            <span className="text-gray-700">{user?.username}</span>
            <button
              onClick={logout}
              className="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-8 sm:px-6 lg:px-8">
        {error && (
          <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-800">{error}</p>
          </div>
        )}

        {/* Pipelines Section */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            Pipelines
          </h2>
          {pipelines.length === 0 ? (
            <p className="text-gray-600">No pipelines found</p>
          ) : (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {pipelines.map((pipeline) => (
                <div
                  key={pipeline.id}
                  className="bg-white p-6 rounded-lg shadow hover:shadow-md transition-shadow"
                >
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    {pipeline.name}
                  </h3>
                  <p className="text-gray-600 mb-4 text-sm">
                    {pipeline.description || 'No description'}
                  </p>
                  <div className="flex items-center justify-between">
                    <span
                      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        pipeline.status === 'completed'
                          ? 'bg-green-100 text-green-800'
                          : pipeline.status === 'running'
                          ? 'bg-blue-100 text-blue-800'
                          : pipeline.status === 'failed'
                          ? 'bg-red-100 text-red-800'
                          : 'bg-gray-100 text-gray-800'
                      }`}
                    >
                      {pipeline.status}
                    </span>
                    <button
                      onClick={() => handleRunPipeline(pipeline.id)}
                      className="text-indigo-600 hover:text-indigo-800 text-sm font-medium"
                    >
                      Run
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* Assets Section */}
        <section>
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Assets</h2>
          {assets.length === 0 ? (
            <p className="text-gray-600">No assets found</p>
          ) : (
            <div className="bg-white shadow rounded-lg overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Type
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Location
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Size
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {assets.map((asset) => (
                    <tr key={asset.id}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {asset.name}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {asset.type}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {asset.location}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {asset.size_bytes
                          ? `${(asset.size_bytes / 1024 / 1024).toFixed(2)} MB`
                          : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
