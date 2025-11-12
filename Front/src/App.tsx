import { useState } from 'react';
import { LoginPage } from './components/LoginPage';
import { MainPage } from './components/MainPage';
import { FileManagementPage } from './components/FileManagementPage';
import { Footer } from './components/Footer';
import { Toaster } from './components/ui/sonner';
import type { FileRecord } from './components/FileHistoryDialog';
import type { DocumentDetail } from './utils/api';

export type CheckResult = {
  originalityScore: number;
  totalWords: number;
  checkedDate: string;
  mainMetricC5?: number; // C5 as main metric
  matches: Array<{
    doc_id: string;
    max_score: number;
    originality_pct: number;
    decision: string;
    details: DocumentDetail;
  }>;
};

type Page = 'checker' | 'files';

export default function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [username, setUsername] = useState('');
  const [fileHistory, setFileHistory] = useState<FileRecord[]>([]);
  const [currentPage, setCurrentPage] = useState<Page>('checker');

  const handleLogin = (user: string) => {
    setIsAuthenticated(true);
    setUsername(user);
  };

  const handleLogout = () => {
    setIsAuthenticated(false);
    setUsername('');
    setCurrentPage('checker');
  };

  const handleAddFile = (file: FileRecord) => {
    setFileHistory(prev => [file, ...prev]);
  };

  const handleClearFiles = () => {
    setFileHistory([]);
  };

  if (!isAuthenticated) {
    return <LoginPage onLogin={handleLogin} />;
  }

  return (
    <>
      <div className="h-screen flex flex-col bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 relative overflow-hidden">
        {/* Animated background patterns */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none opacity-30">
          <div className="absolute top-20 right-20 w-64 h-64 bg-blue-200 rounded-full mix-blend-multiply filter blur-xl" />
          <div className="absolute bottom-20 left-20 w-80 h-80 bg-indigo-200 rounded-full mix-blend-multiply filter blur-xl" />
          <div className="absolute top-1/2 left-1/2 w-72 h-72 bg-purple-200 rounded-full mix-blend-multiply filter blur-xl" />
        </div>

        <div className="flex-1 flex flex-col overflow-hidden relative z-10">
          {currentPage === 'checker' ? (
            <MainPage
              username={username}
              onLogout={handleLogout}
              fileHistory={fileHistory}
              onAddFile={handleAddFile}
              onClearFiles={handleClearFiles}
              onNavigateToFiles={() => setCurrentPage('files')}
            />
          ) : (
            <div className="flex-1 flex flex-col overflow-hidden">
              <header className="border-b bg-white/80 backdrop-blur-sm z-50 shadow-sm">
                <div className="container mx-auto px-4 py-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="text-xl text-slate-800">Global Aeon</div>
                    </div>
                    <div className="flex items-center gap-2 px-3 py-1.5 bg-blue-50 rounded-lg">
                      <span className="text-sm text-blue-900">{username}</span>
                    </div>
                  </div>
                </div>
              </header>
              <main className="flex-1 container mx-auto px-4 py-6 overflow-y-auto">
                <FileManagementPage
                  files={fileHistory}
                  onAddFile={handleAddFile}
                  onClearFiles={handleClearFiles}
                  onBack={() => setCurrentPage('checker')}
                />
              </main>
            </div>
          )}

          {/* Footer - always visible */}
          <Footer />
        </div>
      </div>

      {/* Toast notifications */}
      <Toaster position="top-right" />
    </>
  );
}
