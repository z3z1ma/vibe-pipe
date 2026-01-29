import { AuthProvider } from './hooks/useAuth';
import { LoginPage } from './pages/LoginPage';
import { DashboardPage } from './pages/DashboardPage';

function App() {
  return (
    <AuthProvider>
      {/* Note: In a real app, we'd use BrowserRouter here */}
      <div className="min-h-screen bg-gray-50">
        <div className="p-4">
          <LoginPage />
          <DashboardPage />
        </div>
      </div>
    </AuthProvider>
  );
}

export default App;
