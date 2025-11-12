import { useState } from 'react';
import { motion } from 'motion/react';
import { Header } from './Header';
import { CheckerForm } from './CheckerForm';
import { Results } from './Results';
import type { CheckResult } from '../App';
import type { FileRecord } from './FileHistoryDialog';
import { search } from '../utils/api';
import { toast } from 'sonner@2.0.3';

type Props = {
  username: string;
  onLogout: () => void;
  fileHistory: FileRecord[];
  onAddFile: (file: FileRecord) => void;
  onClearFiles: () => void;
  onNavigateToFiles: () => void;
};

export function MainPage({ username, onLogout, fileHistory, onAddFile, onClearFiles, onNavigateToFiles }: Props) {
  const [result, setResult] = useState<CheckResult | null>(null);
  const [isChecking, setIsChecking] = useState(false);
  const [currentFileName, setCurrentFileName] = useState<string | null>(null);

  const handleCheck = async (text: string, fileName?: string) => {
    setIsChecking(true);
    setResult(null);
    setCurrentFileName(fileName || null);

    try {
      // Call API with query text
      const apiResponse = await search(text);
      
      // Calculate total words
      const wordCount = text.trim().split(/\s+/).filter(Boolean).length;
      
      // Use C5 as main originality metric
      // C5 = count of matching 5-shingles
      // Lower C5 = higher originality (fewer matches)
      const maxC5 = apiResponse.documents.length > 0
        ? Math.max(...apiResponse.documents.map(doc => doc.details.C5))
        : 0;

      // Calculate originality based on originality_pct from API
      const avgOriginality = apiResponse.documents.length > 0
        ? Math.round(
            apiResponse.documents.reduce((sum, doc) => sum + doc.originality_pct, 0) / 
            apiResponse.documents.length
          )
        : 100; // If no matches, 100% original

      const result: CheckResult = {
        originalityScore: avgOriginality,
        totalWords: wordCount,
        checkedDate: new Date().toLocaleDateString('ru-RU'),
        matches: apiResponse.documents.map(doc => ({
          doc_id: doc.doc_id,
          max_score: doc.max_score,
          originality_pct: doc.originality_pct,
          decision: doc.decision,
          details: doc.details,
        })),
        mainMetricC5: maxC5, // C5 as main originality indicator (lower is better)
      };

      setResult(result);
      toast.success(`Проверка завершена! Найдено совпадений: ${apiResponse.docs_found}`);

      // Add to file history if it's a file upload
      if (fileName) {
        const fileRecord: FileRecord = {
          id: Date.now().toString(),
          fileName: fileName,
          uploadDate: new Date().toLocaleDateString('ru-RU'),
          wordCount: wordCount,
          originalityScore: avgOriginality
        };
        onAddFile(fileRecord);
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Ошибка при проверке';
      toast.error(`Ошибка проверки: ${errorMessage}`);
      console.error('Check error:', error);
    } finally {
      setIsChecking(false);
    }
  };

  const handleReset = () => {
    setResult(null);
    setCurrentFileName(null);
  };

  return (
    <>
      {/* Animated background patterns */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none opacity-30">
        <motion.div
          className="absolute top-20 right-20 w-64 h-64 bg-blue-200 rounded-full mix-blend-multiply filter blur-xl"
          animate={{
            x: [0, 50, 0],
            y: [0, 30, 0],
            scale: [1, 1.1, 1],
          }}
          transition={{
            duration: 8,
            repeat: Infinity,
            ease: 'easeInOut'
          }}
        />
        <motion.div
          className="absolute bottom-20 left-20 w-80 h-80 bg-indigo-200 rounded-full mix-blend-multiply filter blur-xl"
          animate={{
            x: [0, -30, 0],
            y: [0, 50, 0],
            scale: [1, 1.2, 1],
          }}
          transition={{
            duration: 10,
            repeat: Infinity,
            ease: 'easeInOut',
            delay: 1
          }}
        />
        <motion.div
          className="absolute top-1/2 left-1/2 w-72 h-72 bg-purple-200 rounded-full mix-blend-multiply filter blur-xl"
          animate={{
            x: [0, 40, 0],
            y: [0, -40, 0],
            scale: [1, 1.15, 1],
          }}
          transition={{
            duration: 12,
            repeat: Infinity,
            ease: 'easeInOut',
            delay: 2
          }}
        />
      </div>

      <Header 
        username={username} 
        onLogout={onLogout}
        fileHistory={fileHistory}
        onClearFiles={onClearFiles}
        onNavigateToFiles={onNavigateToFiles}
      />
      
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5 }}
        className="flex-1 flex flex-col overflow-hidden"
      >
        <main className="flex-1 container mx-auto px-4 py-4 relative z-10 overflow-y-auto">
          {!result ? (
            <CheckerForm onCheck={handleCheck} isChecking={isChecking} />
          ) : (
            <Results result={result} onReset={handleReset} fileName={currentFileName} />
          )}
        </main>
      </motion.div>
    </>
  );
}
