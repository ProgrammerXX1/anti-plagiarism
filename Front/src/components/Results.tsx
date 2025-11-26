import { motion } from 'motion/react';
import { Card } from './ui/card';
import { Button } from './ui/button';
import { Badge } from './ui/badge';
import { Progress } from './ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Alert, AlertDescription } from './ui/alert';
import { ScrollArea } from './ui/scroll-area';
import { CheckCircle2, AlertTriangle, FileText, RotateCcw, TrendingUp } from 'lucide-react';
import type { CheckResult } from '../App';

type Props = {
  result: CheckResult;
  onReset: () => void;
  fileName?: string | null;
};

export function Results({ result, onReset, fileName }: Props) {
  // C5-based color coding (lower C5 = more original = green)
  const getC5Color = (c5: number) => {
    if (c5 === 0) return 'text-green-600';
    if (c5 <= 5) return 'text-green-600';
    if (c5 <= 20) return 'text-amber-600';
    return 'text-red-600';
  };

  const getC5Bg = (c5: number) => {
    if (c5 === 0) return 'from-green-500 to-emerald-600';
    if (c5 <= 5) return 'from-green-500 to-emerald-600';
    if (c5 <= 20) return 'from-amber-500 to-orange-600';
    return 'from-red-500 to-rose-600';
  };

  const getC5Status = (c5: number) => {
    if (c5 === 0) return 'Отлично! Совпадений не найдено';
    if (c5 <= 5) return 'Хорошо! Минимальные совпадения';
    if (c5 <= 20) return 'Внимание! Обнаружены совпадения';
    return 'Критично! Высокий уровень совпадений';
  };

  const getScoreColor = (score: number) => {
    if (score >= 80) return 'text-green-600';
    if (score >= 60) return 'text-amber-600';
    return 'text-red-600';
  };

  const getScoreBg = (score: number) => {
    if (score >= 80) return 'from-green-500 to-emerald-600';
    if (score >= 60) return 'from-amber-500 to-orange-600';
    return 'from-red-500 to-rose-600';
  };

  const getStatusMessage = (score: number) => {
    if (score >= 90) return 'Отлично! Текст оригинальный';
    if (score >= 80) return 'Хорошо! Высокая оригинальность';
    if (score >= 60) return 'Внимание! Обнаружены совпадения';
    return 'Критично! Высокий уровень плагиата';
  };

  const getDecisionBadge = (decision: string) => {
    if (decision === 'original') return { variant: 'default' as const, text: 'Оригинал' };
    if (decision === 'partial') return { variant: 'secondary' as const, text: 'Частичное' };
    return { variant: 'destructive' as const, text: 'Плагиат' };
  };

  const mainC5 = result.mainMetricC5 ?? 0;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.5 }}
      className="max-w-6xl mx-auto space-y-4 h-full overflow-y-auto pb-6"
    >
      {/* Score Overview */}
      <Card className="p-6 shadow-xl border-0 bg-white/80 backdrop-blur-sm">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <motion.div
              initial={{ scale: 0, rotate: -180 }}
              animate={{ scale: 1, rotate: 0 }}
              transition={{ type: 'spring', stiffness: 200 }}
              className={`w-14 h-14 rounded-full bg-gradient-to-br ${getC5Bg(mainC5)} flex items-center justify-center`}
            >
              {mainC5 <= 5 ? (
                <CheckCircle2 className="w-7 h-7 text-white" />
              ) : (
                <AlertTriangle className="w-7 h-7 text-white" />
              )}
            </motion.div>
            <div>
              <h2 className="text-xl mb-1">Результаты проверки</h2>
              <p className="text-xs text-slate-500">
                {fileName && <span className="text-blue-600">{fileName} • </span>}
                Проверено: {result.checkedDate}
              </p>
            </div>
          </div>

          <div className="flex gap-2">
            <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
              <Button variant="outline" size="sm" onClick={onReset}>
                <RotateCcw className="w-4 h-4 mr-2" />
                Новая проверка
              </Button>
            </motion.div>
          </div>
        </div>

        {/* Score Display */}
        <div className="grid md:grid-cols-4 gap-4">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
            className={`text-center p-6 bg-gradient-to-br rounded-xl border-2 ${
              mainC5 === 0 
                ? 'from-green-50 to-emerald-100 border-green-400' 
                : mainC5 <= 5
                ? 'from-green-50 to-emerald-100 border-green-300'
                : mainC5 <= 20
                ? 'from-amber-50 to-orange-100 border-amber-300'
                : 'from-red-50 to-rose-100 border-red-300'
            }`}
          >
            <div className={`text-xs mb-1 ${
              mainC5 <= 5 ? 'text-green-700' : mainC5 <= 20 ? 'text-amber-700' : 'text-red-700'
            }`}>
              ⭐ ПОКАЗАТЕЛЬ ОРИГИНАЛЬНОСТИ
            </div>
            <div className={`text-5xl mb-2 ${getC5Color(mainC5)}`}>
              {mainC5}
            </div>
            <p className="text-sm text-slate-600">C5 (Совпадения 5-шингл)</p>
            <p className="text-xs text-slate-500 mt-1">Чем ниже, тем оригинальнее</p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="text-center p-6 bg-gradient-to-br from-slate-50 to-slate-100 rounded-xl"
          >
            <div className={`text-5xl mb-2 ${getScoreColor(result.originalityScore)}`}>
              {result.originalityScore}%
            </div>
            <p className="text-sm text-slate-600">Оригинальность</p>
            <Progress 
              value={result.originalityScore} 
              className="mt-3 h-2"
            />
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="text-center p-6 bg-gradient-to-br from-blue-50 to-blue-100 rounded-xl"
          >
            <div className="text-5xl text-blue-600 mb-2">
              {result.totalWords}
            </div>
            <p className="text-sm text-slate-600">Всего слов</p>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.4 }}
            className="text-center p-6 bg-gradient-to-br from-amber-50 to-amber-100 rounded-xl"
          >
            <div className="text-5xl text-amber-600 mb-2">
              {result.matches.length}
            </div>
            <p className="text-sm text-slate-600">Совпадений найдено</p>
          </motion.div>
        </div>

        <Alert className={`mt-4 ${
          mainC5 <= 5
            ? 'border-green-200 bg-green-50' 
            : mainC5 <= 20
            ? 'border-amber-200 bg-amber-50'
            : 'border-red-200 bg-red-50'
        }`}>
          <AlertDescription className="flex items-center gap-2">
            {mainC5 <= 5 ? (
              <CheckCircle2 className="w-4 h-4 text-green-600" />
            ) : (
              <AlertTriangle className="w-4 h-4 text-amber-600" />
            )}
            <span className="text-sm">
              <strong>C5 = {mainC5}:</strong> {getC5Status(mainC5)}
            </span>
          </AlertDescription>
        </Alert>
      </Card>

      {/* Detailed Results */}
      <Card className="p-6 shadow-xl border-0 bg-white/80 backdrop-blur-sm">
        <Tabs defaultValue="matches" className="w-full">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="matches">
              Найденные совпадения ({result.matches.length})
            </TabsTrigger>
            <TabsTrigger value="metrics">
              Метрики сходства
            </TabsTrigger>
          </TabsList>

          <TabsContent value="matches" className="mt-4">
            {result.matches.length === 0 ? (
              <div className="text-center py-12">
                <CheckCircle2 className="w-16 h-16 text-green-500 mx-auto mb-4" />
                <h3 className="text-lg mb-2 text-slate-700">
                  Совпадений не найдено
                </h3>
                <p className="text-sm text-slate-500">
                  Текст является оригинальным
                </p>
              </div>
            ) : (
              <ScrollArea className="h-[400px] pr-4">
                <div className="space-y-4">
                  {result.matches.map((match, index) => (
                    <motion.div
                      key={index}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index * 0.05 }}
                    >
                      <Card className="p-4 hover:shadow-md transition-shadow">
                        <div className="flex items-start justify-between gap-4 mb-3">
                          <div className="flex items-center gap-2">
                            <FileText className="w-4 h-4 text-blue-600 flex-shrink-0" />
                            <span className="text-sm text-slate-800">
                              Документ: <strong>{match.doc_id}</strong>
                            </span>
                          </div>
                          <div className="flex gap-2">
                            <Badge variant={getDecisionBadge(match.decision).variant}>
                              {getDecisionBadge(match.decision).text}
                            </Badge>
                            <Badge variant="outline">
                              {match.originality_pct}% оригинально
                            </Badge>
                          </div>
                        </div>

                        {/* Metrics - C5 highlighted as main originality indicator */}
                        <div className="grid grid-cols-3 gap-2 mb-3">
                          <div className={`text-center p-3 bg-gradient-to-br rounded-lg border-2 ${
                            match.details.C5 === 0
                              ? 'from-green-100 to-emerald-100 border-green-500'
                              : match.details.C5 <= 5
                              ? 'from-green-100 to-emerald-100 border-green-400'
                              : match.details.C5 <= 20
                              ? 'from-amber-100 to-orange-100 border-amber-400'
                              : 'from-red-100 to-rose-100 border-red-400'
                          }`}>
                            <div className={`text-xs mb-1 ${
                              match.details.C5 <= 5 ? 'text-green-800' : match.details.C5 <= 20 ? 'text-amber-800' : 'text-red-800'
                            }`}>
                              ⭐ ОРИГИНАЛЬНОСТЬ
                            </div>
                            <div className="text-xs text-slate-700">C5</div>
                            <div className={`text-2xl ${getC5Color(match.details.C5)}`}>
                              {match.details.C5}
                            </div>
                            <div className="text-xs text-slate-600 mt-1">J5: {match.details.J5.toFixed(4)}</div>
                          </div>
                          <div className="text-center p-3 bg-slate-50 rounded-lg">
                            <div className="text-xs text-slate-600 mb-1">C9 / J9</div>
                            <div className="text-lg text-slate-900">
                              {match.details.C9}
                            </div>
                            <div className="text-xs text-slate-600 mt-1">{match.details.J9.toFixed(4)}</div>
                          </div>
                          <div className="text-center p-3 bg-slate-50 rounded-lg">
                            <div className="text-xs text-slate-600 mb-1">C13 / J13</div>
                            <div className="text-lg text-slate-900">
                              {match.details.C13}
                            </div>
                            <div className="text-xs text-slate-600 mt-1">{match.details.J13.toFixed(4)}</div>
                          </div>
                        </div>

                        {/* Matching Fragments */}
                        {match.details.matching_fragments.length > 0 && (
                          <div className="space-y-2">
                            <div className="text-xs text-slate-600 mb-1">
                              Совпадающие фрагменты ({match.details.matching_fragments.length}):
                            </div>
                            {match.details.matching_fragments.map((fragment, fragIdx) => (
                              <div key={fragIdx} className="bg-red-50 border border-red-200 p-3 rounded-lg">
                                <div className="text-xs text-red-700 mb-1">
                                  Позиция: {fragment.start} - {fragment.end}
                                </div>
                                <p className="text-sm text-slate-700 leading-relaxed">
                                  {fragment.text}
                                </p>
                              </div>
                            ))}
                          </div>
                        )}
                      </Card>
                    </motion.div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </TabsContent>

          <TabsContent value="metrics" className="mt-4">
            <ScrollArea className="h-[400px]">
              <div className="space-y-4">
                {result.matches.map((match, index) => (
                  <Card key={index} className="p-4">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-sm">Документ: {match.doc_id}</h4>
                      <Badge variant="outline">Score: {match.max_score.toFixed(6)}</Badge>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                      <div className={`p-4 bg-gradient-to-br rounded-lg border-2 ${
                        match.details.C5 === 0
                          ? 'from-green-100 to-emerald-100 border-green-500'
                          : match.details.C5 <= 5
                          ? 'from-green-100 to-emerald-100 border-green-400'
                          : match.details.C5 <= 20
                          ? 'from-amber-100 to-orange-100 border-amber-400'
                          : 'from-red-100 to-rose-100 border-red-400'
                      }`}>
                        <div className="flex items-center gap-2 mb-2">
                          <TrendingUp className={`w-4 h-4 ${
                            match.details.C5 <= 5 ? 'text-green-700' : match.details.C5 <= 20 ? 'text-amber-700' : 'text-red-700'
                          }`} />
                          <span className={`text-xs ${
                            match.details.C5 <= 5 ? 'text-green-700' : match.details.C5 <= 20 ? 'text-amber-700' : 'text-red-700'
                          }`}>
                            ⭐ C5 (ОРИГИНАЛЬНОСТЬ)
                          </span>
                        </div>
                        <div className={`text-3xl ${getC5Color(match.details.C5)}`}>
                          {match.details.C5}
                        </div>
                        <div className="text-xs text-slate-600 mt-1">Ниже = оригинальнее</div>
                      </div>

                      <div className="p-3 bg-blue-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-1">
                          <TrendingUp className="w-3 h-3 text-blue-600" />
                          <span className="text-xs text-blue-600">J5 (Jaccard 5)</span>
                        </div>
                        <div className="text-lg text-blue-900">{match.details.J5.toFixed(6)}</div>
                      </div>

                      <div className="p-3 bg-purple-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-1">
                          <TrendingUp className="w-3 h-3 text-purple-600" />
                          <span className="text-xs text-purple-600">J9 (Jaccard 9)</span>
                        </div>
                        <div className="text-lg text-purple-900">{match.details.J9.toFixed(6)}</div>
                      </div>

                      <div className="p-3 bg-amber-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-1">
                          <TrendingUp className="w-3 h-3 text-amber-600" />
                          <span className="text-xs text-amber-600">C9 (Count 9)</span>
                        </div>
                        <div className="text-lg text-amber-900">{match.details.C9}</div>
                      </div>

                      <div className="p-3 bg-indigo-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-1">
                          <TrendingUp className="w-3 h-3 text-indigo-600" />
                          <span className="text-xs text-indigo-600">J13 (Jaccard 13)</span>
                        </div>
                        <div className="text-lg text-indigo-900">{match.details.J13.toFixed(6)}</div>
                      </div>

                      <div className="p-3 bg-rose-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-1">
                          <TrendingUp className="w-3 h-3 text-rose-600" />
                          <span className="text-xs text-rose-600">C13 (Count 13)</span>
                        </div>
                        <div className="text-lg text-rose-900">{match.details.C13}</div>
                      </div>

                      <div className="p-3 bg-slate-100 rounded-lg">
                        <div className="text-xs text-slate-600 mb-1">Candidate Hits</div>
                        <div className="text-lg text-slate-900">{match.details.cand_hits}</div>
                      </div>

                      <div className="p-3 bg-slate-100 rounded-lg">
                        <div className="text-xs text-slate-600 mb-1">Hamming SimHash</div>
                        <div className="text-lg text-slate-900">{match.details.hamming_simhash}</div>
                      </div>

                      <div className="p-3 bg-slate-100 rounded-lg">
                        <div className="text-xs text-slate-600 mb-1">MinHash Sim Est</div>
                        <div className="text-lg text-slate-900">{match.details.minhash_sim_est}</div>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            </ScrollArea>
          </TabsContent>
        </Tabs>
      </Card>
    </motion.div>
  );
}
