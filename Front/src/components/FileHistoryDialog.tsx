import { motion } from 'motion/react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from './ui/dialog';
import { Button } from './ui/button';
import { ScrollArea } from './ui/scroll-area';
import { Badge } from './ui/badge';
import { Card } from './ui/card';
import { FileText, Trash2, Calendar, FileCheck, AlertTriangle } from 'lucide-react';
import { Alert, AlertDescription } from './ui/alert';
import { useState } from 'react';
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

export type FileRecord = {
  id: string;
  fileName: string;
  uploadDate: string;
  wordCount: number;
  originalityScore?: number;
  doc_id?: string; // Backend document ID
  chars?: number;
  tokens?: number;
};

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  files: FileRecord[];
  onClearAll: () => void;
};

export function FileHistoryDialog({ open, onOpenChange, files, onClearAll }: Props) {
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  const handleClearAll = () => {
    onClearAll();
    setShowDeleteConfirm(false);
  };

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-3xl max-h-[80vh]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileText className="w-5 h-5 text-blue-600" />
              История загруженных файлов
            </DialogTitle>
            <DialogDescription>
              Просмотр всех проверенных документов
            </DialogDescription>
          </DialogHeader>

          {files.length === 0 ? (
            <motion.div
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              className="text-center py-12"
            >
              <FileText className="w-16 h-16 text-slate-300 mx-auto mb-4" />
              <h3 className="text-lg mb-2 text-slate-700">Нет загруженных файлов</h3>
              <p className="text-sm text-slate-500">
                Загрузите файлы для проверки, и они появятся здесь
              </p>
            </motion.div>
          ) : (
            <>
              <div className="flex items-center justify-between mb-4">
                <div className="text-sm text-slate-600">
                  Всего файлов: <strong>{files.length}</strong>
                </div>
                <Button
                  variant="destructive"
                  size="sm"
                  onClick={() => setShowDeleteConfirm(true)}
                >
                  <Trash2 className="w-4 h-4 mr-2" />
                  Очистить всю базу
                </Button>
              </div>

              <ScrollArea className="h-[400px] pr-4">
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
                              <FileCheck className="w-4 h-4 text-blue-600" />
                              <h4 className="text-sm text-slate-800 truncate">
                                {file.fileName}
                              </h4>
                            </div>

                            <div className="flex flex-wrap items-center gap-3 text-xs text-slate-600">
                              <div className="flex items-center gap-1">
                                <Calendar className="w-3 h-3" />
                                {file.uploadDate}
                              </div>
                              <div>
                                Слов: <strong>{file.wordCount}</strong>
                              </div>
                              {file.originalityScore !== undefined && (
                                <Badge
                                  variant={
                                    file.originalityScore >= 80
                                      ? 'default'
                                      : file.originalityScore >= 60
                                      ? 'secondary'
                                      : 'destructive'
                                  }
                                >
                                  Оригинальность: {file.originalityScore}%
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
            </>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Закрыть
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

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
                  <strong>Внимание:</strong> Будет удалено {files.length} {files.length === 1 ? 'файл' : 'файлов'} из базы данных.
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
    </>
  );
}
