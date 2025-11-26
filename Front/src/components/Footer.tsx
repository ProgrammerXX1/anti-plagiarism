import { motion } from 'motion/react';
import { Logo } from './Logo';

export function Footer() {
  return (
    <motion.footer
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay: 0.5 }}
      className="bg-slate-900 text-slate-300 z-50"
    >
      <div className="container mx-auto px-4 py-3">
        <div className="flex flex-col md:flex-row items-center justify-between gap-2">
          <div className="scale-75 origin-left">
            <Logo size="sm" showText={true} variant="dark" />
          </div>

          <p className="text-xs text-slate-400">
            © 2025 Global Aeon. Все права защищены.
          </p>
        </div>
      </div>
    </motion.footer>
  );
}
