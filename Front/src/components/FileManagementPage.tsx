import { useState } from 'react';
import { motion } from 'motion/react';
import { Card } from './ui/card';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Badge } from './ui/badge';
import { ScrollArea } from './ui/scroll-area';
import { Alert, AlertDescription } from './ui/alert';
import { 
  Upload, 
  FileText, 
  Trash2, 
  Calendar, 
  FileCheck, 
  AlertTriangle,
  FolderOpen,
  ArrowLeft,
  Loader2,
  CheckCircle2,
  Database
} from 'lucide-react';
import type { FileRecord } from './FileHistoryDialog';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from './ui/alert-dialog';
import { uploadFile, buildIndex, resetAll } from '../utils/api';
import { toast } from 'sonner@2.0.3';

type Props = {
  files: FileRecord[];
  onAddFile: (file: FileRecord) => void;
  onClearFiles: () => void;
  onBack: () => void;
};

export function FileManagementPage({ files, onAddFile, onClearFiles, onBack }: Props) {
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [isBuilding, setIsBuilding] = useState(false);
  const [uploadedCount, setUploadedCount] = useState(0);
  const [totalFiles, setTotalFiles] = useState(0);

  const handleFileUpload = async (fileList: FileList | null) => {
    if (!fileList || fileList.length === 0) return;

    setIsProcessing(true);
    setUploadedCount(0);
    setTotalFiles(fileList.length);

    const filesArray = Array.from(fileList);
    let successCount = 0;

    for (let i = 0; i < filesArray.length; i++) {
      const file = filesArray[i];
      
      try {
        const uploadResponse = await uploadFile(file, true);
        
        const fileRecord: FileRecord = {
          id: uploadResponse.doc_id || Date.now().toString(),
          fileName: file.name,
          uploadDate: new Date().toLocaleDateString('ru-RU'),
          wordCount: 0,
          doc_id: uploadResponse.doc_id,
          chars: uploadResponse.bytes || 0,
          tokens: 0
        };

        onAddFile(fileRecord);
        successCount++;
        setUploadedCount(i + 1);
        
        toast.success(`✓ Загружен: ${file.name}`);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Не удалось загрузить файл';
        toast.error(`✗ ${file.name}: ${errorMessage}`);
      }
    }

    setIsProcessing(false);
    setTotalFiles(0);
    setUploadedCount(0);

    if (successCount > 0) {
      toast.success(`✓ Успешно загружено ${successCount} из ${filesArray.length} файлов`);
    }
  };

  const handleBuildIndex = async () => {
    setIsBuilding(true);
    
    try {
      const result = await buildIndex();
      
      toast.success(
        `✓ Индекс построен! Документов: ${result.docs}, Шинглов: k5=${result.k5}, k9=${result.k9}, k13=${result.k13}`,
        { duration: 5000 }
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Не удалось построить индекс';
      toast.error(`✗ Ошибка построения индекса: ${errorMessage}`);
    } finally {
      setIsBuilding(false);
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
    handleFileUpload(e.dataTransfer.files);
  };

  const handleClearAll = async () => {
    try {
      const result = await resetAll();
      
      onClearFiles();
      toast.success(`✓ База данных очищена. Удалено файлов: ${result.removed?.length || 0}`);
      setShowDeleteConfirm(false);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Не удалось очистить базу';
      toast.error(`✗ Ошибка очистки: ${errorMessage}`);
    }
  };

  return (
    <div className="min-h-full">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-6"
      >
        <Button
          variant="ghost"
          onClick={onBack}
          className="mb-4"
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Назад к проверке
        </Button>

        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-blue-600 to-indigo-600 rounded-xl flex items-center justify-center">
              <FolderOpen className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-2xl">Управление файлами</h1>
              <p className="text-sm text-slate-600">Загружайте и управляйте вашими документами</p>
            </div>
          </div>

          <div className="flex gap-2">
            <Button
              variant="default"
              onClick={handleBuildIndex}
              disabled={isBuilding || files.length === 0}
              className="bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700"
            >
              {isBuilding ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Построение...
                </>
              ) : (
                <>
                  <Database className="w-4 h-4 mr-2" />
                  Построить индекс
                </>
              )}
            </Button>

            {files.length > 0 && (
              <Button
                variant="destructive"
                onClick={() => setShowDeleteConfirm(true)}
              >
                <Trash2 className="w-4 h-4 mr-2" />
                Очистить базу
              </Button>
            )}
          </div>
        </div>
      </motion.div>

      <div className="grid lg:grid-cols-2 gap-6">
        {/* Upload Section */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.1 }}
        >
          <Card className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <Upload className="w-5 h-5 text-blue-600" />
              <h2 className="text-lg">Загрузка файлов</h2>
            </div>

            {/* Drag & Drop Area */}
            <div
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
              className={`
                border-2 border-dashed rounded-lg p-8 text-center transition-all
                ${isDragging 
                  ? 'border-blue-500 bg-blue-50' 
                  : 'border-slate-300 hover:border-blue-400 hover:bg-slate-50'
                }
              `}
            >
              <motion.div
                animate={{ y: isDragging ? -5 : 0 }}
                transition={{ type: 'spring', stiffness: 300 }}
              >
                <div className="w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  {isProcessing ? (
                    <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
                  ) : (
                    <Upload className="w-8 h-8 text-blue-600" />
                  )}
                </div>
                <h3 className="mb-2 text-slate-800">
                  {isProcessing 
                    ? `Обработка файлов... (${uploadedCount}/${totalFiles})` 
                    : isDragging 
                    ? 'Отпустите файлы здесь' 
                    : 'Перетащите файлы сюда'}
                </h3>
                <p className="text-sm text-slate-600 mb-4">
                  или нажмите кнопку ниже
                </p>
                <label htmlFor="file-upload">
                  <Button asChild variant="outline" disabled={isProcessing}>
                    <span>
                      <FileText className="w-4 h-4 mr-2" />
                      Выбрать файлы
                    </span>
                  </Button>
                </label>
                <Input
                  id="file-upload"
                  type="file"
                  accept=".txt,.pdf,.doc,.docx,text/plain,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                  multiple
                  className="hidden"
                  onChange={(e) => handleFileUpload(e.target.files)}
                  disabled={isProcessing}
                />
              </motion.div>
            </div>

            <Alert className="mt-4 border-blue-200 bg-blue-50">
              <AlertDescription className="text-sm text-slate-700">
                Поддерживаемые форматы: <strong>.txt, .pdf, .doc, .docx</strong>
              </AlertDescription>
            </Alert>

            {files.length > 0 && (
              <Alert className="mt-4 border-green-200 bg-green-50">
                <CheckCircle2 className="w-4 h-4 text-green-600" />
                <AlertDescription className="text-sm text-slate-700">
                  После загрузки файлов нажмите <strong>"Построить индекс"</strong> для подготовки к поиску
                </AlertDescription>
              </Alert>
            )}
          </Card>
        </motion.div>

        {/* Files List Section */}
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
        >
          <Card className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <FileCheck className="w-5 h-5 text-blue-600" />
                <h2 className="text-lg">Загруженные файлы</h2>
              </div>
              <Badge variant="secondary">
                {files.length} {files.length === 1 ? 'файл' : 'файлов'}
              </Badge>
            </div>

            {files.length === 0 ? (
              <div className="text-center py-12">
                <FileText className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                <h3 className="text-lg mb-2 text-slate-700">Нет файлов</h3>
                <p className="text-sm text-slate-500">
                  Загрузите файлы, чтобы они появились здесь
                </p>
              </div>
            ) : (
              <ScrollArea className="h-[500px] pr-4">
                <div className="space-y-3">
                  {files.map((file, index) => (
                    <motion.div
                      key={file.id}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index * 0.05 }}
                    >
                      <Card className="p-4 hover:shadow-md transition-shadow">
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-2">
                              <FileCheck className="w-4 h-4 text-blue-600 flex-shrink-0" />
                              <h4 className="text-sm text-slate-800 truncate">
                                {file.fileName}
                              </h4>
                            </div>

                            <div className="flex flex-wrap items-center gap-3 text-xs text-slate-600">
                              <div className="flex items-center gap-1">
                                <Calendar className="w-3 h-3" />
                                {file.uploadDate}
                              </div>
                              {file.doc_id && (
                                <div className="text-xs text-blue-600">
                                  ID: {file.doc_id}
                                </div>
                              )}
                              {file.originalityScore !== undefined && (
                                <Badge
                                  variant={
                                    file.originalityScore >= 80
                                      ? 'default'
                                      : file.originalityScore >= 60
                                      ? 'secondary'
                                      : 'destructive'
                                  }
                                  className="text-xs"
                                >
                                  {file.originalityScore}%
                                </Badge>
                              )}
                            </div>
                          </div>
                        </div>
                      </Card>
                    </motion.div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </Card>
        </motion.div>
      </div>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={showDeleteConfirm} onOpenChange={setShowDeleteConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="w-5 h-5 text-red-600" />
              Подтверждение удаления
            </AlertDialogTitle>
            <AlertDialogDescription>
              Вы уверены, что хотите удалить всю базу загруженных файлов? Это действие нельзя отменить.
              <Alert className="mt-4 border-red-200 bg-red-50">
                <AlertDescription className="text-sm">
                  <strong>Внимание:</strong> Будет удалено {files.length} {files.length === 1 ? 'файл' : 'файлов'} из базы данных и индекс.
                </AlertDescription>
              </Alert>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Отмена</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleClearAll}
              className="bg-red-600 hover:bg-red-700"
            >
              Да, удалить всё
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
