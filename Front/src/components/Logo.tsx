import { motion } from 'motion/react';

type Props = {
  size?: 'sm' | 'md' | 'lg';
  showText?: boolean;
  variant?: 'light' | 'dark';
};

export function Logo({ size = 'md', showText = true, variant = 'light' }: Props) {
  const sizes = {
    sm: { icon: 32, text: 'text-lg' },
    md: { icon: 40, text: 'text-xl' },
    lg: { icon: 56, text: 'text-4xl' }
  };

  const iconSize = sizes[size].icon;
  const textSize = sizes[size].text;

  return (
    <div className="flex items-center gap-3">
      {/* Animated Logo Icon */}
      <motion.div
        initial={{ scale: 0, rotate: -180 }}
        animate={{ scale: 1, rotate: 0 }}
        transition={{ 
          type: 'spring',
          stiffness: 260,
          damping: 20,
          duration: 0.6 
        }}
        className="relative"
        style={{ width: iconSize, height: iconSize }}
      >
        <svg
          width={iconSize}
          height={iconSize}
          viewBox="0 0 100 100"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
        >
          {/* Outer rotating ring */}
          <motion.circle
            cx="50"
            cy="50"
            r="42"
            stroke="url(#gradient1)"
            strokeWidth="3"
            fill="none"
            strokeLinecap="round"
            strokeDasharray="20 10"
            initial={{ rotate: 0 }}
            animate={{ rotate: 360 }}
            transition={{
              duration: 20,
              repeat: Infinity,
              ease: 'linear'
            }}
            style={{ originX: '50px', originY: '50px' }}
          />

          {/* Middle ring - opposite direction */}
          <motion.circle
            cx="50"
            cy="50"
            r="32"
            stroke="url(#gradient2)"
            strokeWidth="2.5"
            fill="none"
            strokeLinecap="round"
            strokeDasharray="15 8"
            initial={{ rotate: 0 }}
            animate={{ rotate: -360 }}
            transition={{
              duration: 15,
              repeat: Infinity,
              ease: 'linear'
            }}
            style={{ originX: '50px', originY: '50px' }}
          />

          {/* Inner orbiting dots */}
          <motion.g
            initial={{ rotate: 0 }}
            animate={{ rotate: 360 }}
            transition={{
              duration: 10,
              repeat: Infinity,
              ease: 'linear'
            }}
            style={{ originX: '50px', originY: '50px' }}
          >
            <circle cx="50" cy="22" r="3" fill="url(#gradient3)">
              <animate
                attributeName="r"
                values="3;4;3"
                dur="2s"
                repeatCount="indefinite"
              />
            </circle>
            <circle cx="50" cy="78" r="3" fill="url(#gradient3)">
              <animate
                attributeName="r"
                values="3;4;3"
                dur="2s"
                repeatCount="indefinite"
                begin="0.5s"
              />
            </circle>
          </motion.g>

          {/* Center core with pulse */}
          <motion.circle
            cx="50"
            cy="50"
            r="8"
            fill="url(#gradient4)"
            animate={{
              scale: [1, 1.1, 1],
              opacity: [1, 0.8, 1]
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: 'easeInOut'
            }}
            style={{ originX: '50px', originY: '50px' }}
          />

          {/* Gradients */}
          <defs>
            <linearGradient id="gradient1" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#3b82f6" />
              <stop offset="100%" stopColor="#6366f1" />
            </linearGradient>
            <linearGradient id="gradient2" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#6366f1" />
              <stop offset="100%" stopColor="#8b5cf6" />
            </linearGradient>
            <linearGradient id="gradient3" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#8b5cf6" />
              <stop offset="100%" stopColor="#a855f7" />
            </linearGradient>
            <radialGradient id="gradient4">
              <stop offset="0%" stopColor="#60a5fa" />
              <stop offset="100%" stopColor="#3b82f6" />
            </radialGradient>
          </defs>
        </svg>
      </motion.div>

      {/* Text */}
      {showText && (
        <motion.div
          initial={{ opacity: 0, x: -10 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.3, duration: 0.5 }}
        >
          <h1 className={`${textSize} ${variant === 'light' ? 'text-slate-800' : 'text-white'}`}>
            Global Aeon
          </h1>
          {size !== 'sm' && (
            <p className={`text-xs ${variant === 'light' ? 'text-indigo-600' : 'text-blue-200'}`}>
              Проверка на оригинальность
            </p>
          )}
        </motion.div>
      )}
    </div>
  );
}
