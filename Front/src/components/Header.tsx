import { motion } from 'motion/react';
import { LogOut, User, FolderOpen } from 'lucide-react';
import { Button } from './ui/button';
import { Logo } from './Logo';
import { Badge } from './ui/badge';
import type { FileRecord } from './FileHistoryDialog';

type Props = {
  username: string;
  onLogout: () => void;
  fileHistory: FileRecord[];
  onClearFiles: () => void;
  onNavigateToFiles: () => void;
};

export function Header({ username, onLogout, fileHistory, onNavigateToFiles }: Props) {
  return (
    <motion.header
      initial={{ y: -100 }}
      animate={{ y: 0 }}
      transition={{ type: 'spring', stiffness: 100 }}
      className="border-b bg-white/80 backdrop-blur-sm z-50 shadow-sm"
    >
      <div className="container mx-auto px-4 py-3 flex items-center justify-between">
        <motion.div whileHover={{ scale: 1.05 }}>
          <Logo size="sm" showText={true} variant="light" />
        </motion.div>

        <div className="flex items-center gap-3">
          <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
            <Button
              variant="outline"
              size="sm"
              onClick={onNavigateToFiles}
              className="relative"
            >
              <FolderOpen className="w-4 h-4 mr-2" />
              Управление файлами
              {fileHistory.length > 0 && (
                <Badge
                  variant="default"
                  className="ml-2 px-1.5 py-0 h-5 text-xs bg-blue-600"
                >
                  {fileHistory.length}
                </Badge>
              )}
            </Button>
          </motion.div>

          <div className="flex items-center gap-2 px-3 py-1.5 bg-blue-50 rounded-lg">
            <User className="w-4 h-4 text-blue-600" />
            <span className="text-sm text-blue-900">{username}</span>
          </div>

          <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
            <Button variant="outline" size="sm" onClick={onLogout}>
              <LogOut className="w-4 h-4 mr-2" />
              Выйти
            </Button>
          </motion.div>
        </div>
      </div>
    </motion.header>
  );
}
