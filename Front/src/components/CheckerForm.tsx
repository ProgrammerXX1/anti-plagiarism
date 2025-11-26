import { useState } from 'react';
import { motion } from 'motion/react';
import { Button } from './ui/button';
import { Textarea } from './ui/textarea';
import { Card } from './ui/card';
import { Upload, FileText, Loader2, Sparkles, FileUp, X } from 'lucide-react';
import { Input } from './ui/input';
import { toast } from 'sonner@2.0.3';

type Props = {
  onCheck: (text: string, fileName?: string) => void;
  isChecking: boolean;
};

export function CheckerForm({ onCheck, isChecking }: Props) {
  const [text, setText] = useState('');
  const [uploadedFileName, setUploadedFileName] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [isProcessingFile, setIsProcessingFile] = useState(false);
  const wordCount = text.trim().split(/\s+/).filter(Boolean).length;
  const charCount = text.length;

  // Simple text extraction for text files
  const extractText = async (file: File): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => resolve(e.target?.result as string || '');
      reader.onerror = reject;
      reader.readAsText(file);
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (text.trim()) {
      onCheck(text, uploadedFileName || undefined);
    }
  };

  const handlePaste = async () => {
    try {
      const clipboardText = await navigator.clipboard.readText();
      setText(clipboardText);
      setUploadedFileName(null);
    } catch (err) {
      // Clipboard API not available or permission denied - silently ignore
      // User can manually paste using Ctrl+V
    }
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      processFile(file);
    }
  };

  const processFile = async (file: File) => {
    setIsProcessingFile(true);
    setUploadedFileName(file.name);
    
    try {
      const content = await extractText(file);
      setText(content);
      toast.success(`Файл "${file.name}" загружен. Нажмите "Проверить" для поиска совпадений.`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Не удалось прочитать файл';
      toast.error(errorMessage);
      setUploadedFileName(null);
    } finally {
      setIsProcessingFile(false);
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    
    const file = e.dataTransfer.files?.[0];
    if (file) {
      processFile(file);
    }
  };

  const clearFile = () => {
    setUploadedFileName(null);
    setText('');
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="max-w-5xl mx-auto h-full flex items-center"
    >
      <motion.div
        whileHover={{ scale: 1.01 }}
        transition={{ duration: 0.2 }}
        className="w-full"
      >
        <Card 
          className={`p-6 shadow-xl border-0 bg-white/80 backdrop-blur-sm relative overflow-hidden transition-all ${
            isDragging ? 'ring-2 ring-blue-500 ring-offset-2' : ''
          }`}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
        >
          {/* Decorative elements */}
          <motion.div
            className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-br from-blue-400/20 to-indigo-400/20 rounded-full blur-2xl"
            animate={{
              scale: [1, 1.2, 1],
              opacity: [0.3, 0.5, 0.3],
            }}
            transition={{
              duration: 3,
              repeat: Infinity,
              ease: 'easeInOut'
            }}
          />

          {isDragging && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="absolute inset-0 bg-blue-50/80 backdrop-blur-sm z-10 flex items-center justify-center rounded-lg"
            >
              <div className="text-center">
                <Upload className="w-16 h-16 text-blue-600 mx-auto mb-4" />
                <h3 className="text-xl text-blue-900">Отпустите файл для загрузки</h3>
                <p className="text-sm text-blue-700 mt-2">Поддерживаемые форматы: .txt, .pdf, .doc, .docx</p>
              </div>
            </motion.div>
          )}
          
          <form onSubmit={handleSubmit}>
            <div className="mb-4">
              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.1 }}
                className="flex items-center justify-between mb-2"
              >
                <label htmlFor="text-input" className="flex items-center gap-2">
                  <motion.div
                    animate={{ rotate: [0, 5, -5, 0] }}
                    transition={{ 
                      duration: 2,
                      repeat: Infinity,
                      ease: 'easeInOut'
                    }}
                  >
                    <FileText className="w-5 h-5 text-blue-600" />
                  </motion.div>
                  <span>
                    {uploadedFileName ? (
                      <span className="flex items-center gap-2">
                        <span className="text-blue-600">Файл: {uploadedFileName}</span>
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={clearFile}
                          className="h-6 px-2"
                        >
                          <X className="w-3 h-3" />
                        </Button>
                      </span>
                    ) : (
                      'Введите текст или перетащите файл для проверки'
                    )}
                  </span>
                </label>
                <div className="flex gap-2">
                  <motion.div
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={handlePaste}
                      disabled={isChecking}
                    >
                      <Sparkles className="w-4 h-4 mr-2" />
                      Вставить
                    </Button>
                  </motion.div>
                  <motion.div
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <label htmlFor="file-input">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={isChecking || isProcessingFile}
                        asChild
                      >
                        <span>
                          {isProcessingFile ? (
                            <>
                              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                              Обработка...
                            </>
                          ) : (
                            <>
                              <FileUp className="w-4 h-4 mr-2" />
                              Загрузить
                            </>
                          )}
                        </span>
                      </Button>
                    </label>
                    <Input
                      id="file-input"
                      type="file"
                      accept=".txt,.pdf,.doc,.docx,text/plain,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                      className="hidden"
                      onChange={handleFileUpload}
                      disabled={isChecking || isProcessingFile}
                    />
                  </motion.div>
                </div>
              </motion.div>

              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
              >
                <Textarea
                  id="text-input"
                  placeholder="Введите текст для проверки на плагиат или перетащите файл (.txt, .pdf, .doc, .docx) в эту область..."
                  value={text}
                  onChange={(e) => {
                    setText(e.target.value);
                    if (uploadedFileName && e.target.value !== text) {
                      setUploadedFileName(null);
                    }
                  }}
                  className="min-h-[300px] resize-none text-sm leading-relaxed"
                  disabled={isChecking || isProcessingFile}
                />
              </motion.div>

              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.3 }}
                className="flex justify-between items-center mt-2 text-xs text-slate-500"
              >
                <span>
                  Слов: <strong className="text-slate-700">{wordCount}</strong> | 
                  Символов: <strong className="text-slate-700">{charCount}</strong>
                </span>
                {uploadedFileName && (
                  <span className="text-blue-600">
                    📎 {uploadedFileName}
                  </span>
                )}
              </motion.div>
            </div>

            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.4 }}
              whileHover={{ scale: isChecking ? 1 : 1.02 }}
              whileTap={{ scale: isChecking ? 1 : 0.98 }}
            >
              <Button
                type="submit"
                className="w-full h-12 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                disabled={!text.trim() || isChecking}
              >
                {isChecking ? (
                  <>
                    <motion.div
                      animate={{ rotate: 360 }}
                      transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}
                    >
                      <Loader2 className="w-5 h-5 mr-2" />
                    </motion.div>
                    Проверка...
                  </>
                ) : (
                  <>
                    <Upload className="w-5 h-5 mr-2" />
                    Проверить на плагиат
                  </>
                )}
              </Button>
            </motion.div>
          </form>
        </Card>
      </motion.div>
    </motion.div>
  );
}
