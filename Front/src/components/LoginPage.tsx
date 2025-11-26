import { useState } from 'react';
import { motion } from 'motion/react';
import { Lock, User, AlertCircle, CheckCircle, FileCheck, Sparkles } from 'lucide-react';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Card } from './ui/card';
import { Alert, AlertDescription } from './ui/alert';
import { ImageWithFallback } from './figma/ImageWithFallback';
import { Logo } from './Logo';

type Props = {
  onLogin: (username: string) => void;
};

export function LoginPage({ onLogin }: Props) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);

    // Simulate API call
    await new Promise(resolve => setTimeout(resolve, 1000));

    if (username === 'beka' && password === '2123') {
      onLogin(username);
    } else {
      setError('Неверный логин или пароль');
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 flex relative overflow-hidden">
      {/* Animated background elements - Smooth floating shapes */}
      <div className="absolute inset-0 overflow-hidden">
        <motion.div
          className="absolute w-64 h-64 bg-blue-200/30 rounded-full blur-3xl"
          animate={{
            x: [0, 30, -30, 0],
            y: [0, -50, 50, 0],
            scale: [1, 1.1, 0.9, 1],
          }}
          transition={{
            duration: 25,
            repeat: Infinity,
            ease: 'easeInOut'
          }}
          style={{ top: '20%', left: '15%' }}
        />
        <motion.div
          className="absolute w-96 h-96 bg-indigo-200/30 rounded-full blur-3xl"
          animate={{
            x: [0, -40, 40, 0],
            y: [0, 60, -60, 0],
            scale: [1, 0.9, 1.1, 1],
          }}
          transition={{
            duration: 30,
            repeat: Infinity,
            ease: 'easeInOut',
            delay: 2
          }}
          style={{ bottom: '20%', right: '15%' }}
        />
        <motion.div
          className="absolute w-80 h-80 bg-purple-200/30 rounded-full blur-3xl"
          animate={{
            x: [0, 50, -50, 0],
            y: [0, -40, 40, 0],
            scale: [1, 1.2, 0.8, 1],
          }}
          transition={{
            duration: 35,
            repeat: Infinity,
            ease: 'easeInOut',
            delay: 4
          }}
          style={{ top: '50%', left: '50%' }}
        />
      </div>

      {/* Left Side - Images & Branding */}
      <motion.div
        initial={{ opacity: 0, x: -50 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.8, ease: 'easeOut' }}
        className="hidden lg:flex lg:w-1/2 relative z-10 p-12 flex-col justify-between"
      >
        <div>
          <Logo size="lg" showText={true} variant="light" />

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5, duration: 0.6 }}
            className="space-y-6 mb-8 mt-8"
          >
            <motion.div
              whileHover={{ x: 5 }}
              transition={{ type: 'spring', stiffness: 300 }}
              className="flex items-start gap-3"
            >
              <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center flex-shrink-0 shadow-md">
                <CheckCircle className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <h3 className="text-slate-800 text-lg mb-1">99% точность</h3>
                <p className="text-slate-600 text-sm">Проверка по миллиардам источников в интернете</p>
              </div>
            </motion.div>

            <motion.div
              whileHover={{ x: 5 }}
              transition={{ type: 'spring', stiffness: 300 }}
              className="flex items-start gap-3"
            >
              <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center flex-shrink-0 shadow-md">
                <FileCheck className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <h3 className="text-slate-800 text-lg mb-1">Мгновенные результаты</h3>
                <p className="text-slate-600 text-sm">Получите детальный отчёт за несколько секунд</p>
              </div>
            </motion.div>

            <motion.div
              whileHover={{ x: 5 }}
              transition={{ type: 'spring', stiffness: 300 }}
              className="flex items-start gap-3"
            >
              <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center flex-shrink-0 shadow-md">
                <Sparkles className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <h3 className="text-slate-800 text-lg mb-1">Безопасность данных</h3>
                <p className="text-slate-600 text-sm">Ваши тексты защищены и не сохраняются</p>
              </div>
            </motion.div>
          </motion.div>
        </div>

        {/* Image Grid - 4 photos */}
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.7, duration: 0.6 }}
          className="grid grid-cols-2 gap-4"
        >
          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.8, duration: 0.5 }}
            whileHover={{ scale: 1.05, y: -5 }}
            className="relative h-36 rounded-xl overflow-hidden shadow-lg"
          >
            <ImageWithFallback
              src="https://images.unsplash.com/photo-1637164153353-ef260e2e7e7e?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHx0ZWFtJTIwd29ya2luZyUyMG9mZmljZXxlbnwxfHx8fDE3NjI3MjU0MDB8MA&ixlib=rb-4.1.0&q=80&w=1080"
              alt="Team working"
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-blue-900/40 to-transparent" />
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.9, duration: 0.5 }}
            whileHover={{ scale: 1.05, y: -5 }}
            className="relative h-36 rounded-xl overflow-hidden shadow-lg"
          >
            <ImageWithFallback
              src="https://images.unsplash.com/photo-1594027554094-99c00129af63?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzdHVkZW50JTIwc3R1ZHlpbmclMjB1bml2ZXJzaXR5fGVufDF8fHx8MTc2Mjc1ODg0N3ww&ixlib=rb-4.1.0&q=80&w=1080"
              alt="Student studying"
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-indigo-900/40 to-transparent" />
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 1.0, duration: 0.5 }}
            whileHover={{ scale: 1.05, y: -5 }}
            className="relative h-36 rounded-xl overflow-hidden shadow-lg"
          >
            <ImageWithFallback
              src="https://images.unsplash.com/photo-1641204899528-e28fc426708d?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxsaWJyYXJ5JTIwYm9va3MlMjBhY2FkZW1pY3xlbnwxfHx8fDE3NjI3NzE0NDN8MA&ixlib=rb-4.1.0&q=80&w=1080"
              alt="Library books"
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-purple-900/40 to-transparent" />
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 1.1, duration: 0.5 }}
            whileHover={{ scale: 1.05, y: -5 }}
            className="relative h-36 rounded-xl overflow-hidden shadow-lg"
          >
            <ImageWithFallback
              src="https://images.unsplash.com/photo-1625225230517-7426c1be750c?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkb2N1bWVudCUyMHdyaXRpbmclMjBkZXNrfGVufDF8fHx8MTc2Mjc3MTQ0NHww&ixlib=rb-4.1.0&q=80&w=1080"
              alt="Writing documents"
              className="w-full h-full object-cover"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-blue-900/40 to-transparent" />
          </motion.div>
        </motion.div>
      </motion.div>

      {/* Right Side - Login Form */}
      <motion.div
        initial={{ opacity: 0, x: 50 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.8, ease: 'easeOut' }}
        className="w-full lg:w-1/2 flex items-center justify-center p-4 md:p-8 relative z-10"
      >
        <div className="w-full max-w-md">
          {/* Mobile Logo */}
          <div className="lg:hidden flex items-center justify-center mb-8">
            <Logo size="lg" showText={true} variant="light" />
          </div>

          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.3, duration: 0.5 }}
          >
            <Card className="p-8 shadow-2xl border-0 bg-white/95 backdrop-blur-sm">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.4, duration: 0.5 }}
                className="text-center mb-8"
              >
                <h2 className="text-2xl mb-2 text-slate-800">Добро пожаловать</h2>
                <p className="text-slate-600">Войдите в свой аккаунт</p>
              </motion.div>

              <form onSubmit={handleSubmit} className="space-y-6">
                {error && (
                  <motion.div
                    initial={{ opacity: 0, x: -10, scale: 0.95 }}
                    animate={{ opacity: 1, x: 0, scale: 1 }}
                    transition={{ type: 'spring', stiffness: 300 }}
                  >
                    <Alert variant="destructive">
                      <AlertCircle className="h-4 w-4" />
                      <AlertDescription>{error}</AlertDescription>
                    </Alert>
                  </motion.div>
                )}

                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.5, duration: 0.5 }}
                  className="space-y-2"
                >
                  <Label htmlFor="username">Логин</Label>
                  <div className="relative">
                    <User className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
                    <Input
                      id="username"
                      type="text"
                      placeholder="Введите логин"
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      className="pl-10"
                      required
                    />
                  </div>
                </motion.div>

                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.6, duration: 0.5 }}
                  className="space-y-2"
                >
                  <Label htmlFor="password">Пароль</Label>
                  <div className="relative">
                    <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
                    <Input
                      id="password"
                      type="password"
                      placeholder="Введите пароль"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="pl-10"
                      required
                    />
                  </div>
                </motion.div>

                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.7, duration: 0.5 }}
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <Button
                    type="submit"
                    className="w-full h-12 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                    disabled={isLoading}
                  >
                    {isLoading ? (
                      <motion.div
                        className="flex items-center gap-2"
                      >
                        <motion.div
                          animate={{ rotate: 360 }}
                          transition={{ duration: 1, repeat: Infinity, ease: 'linear' }}
                        >
                          <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full" />
                        </motion.div>
                        Вход...
                      </motion.div>
                    ) : (
                      'Войти'
                    )}
                  </Button>
                </motion.div>
              </form>
            </Card>
          </motion.div>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.9, duration: 0.5 }}
            className="text-center mt-6 text-slate-600 text-sm"
          >
            © 2025 Global Aeon. Все права защищены.
          </motion.p>
        </div>
      </motion.div>
    </div>
  );
}
