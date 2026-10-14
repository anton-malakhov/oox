'use strict';
(() => {
  const pairs={
    EIGEN_STEALING:['eigen','Stealing: сохранённый экспериментальный partitioner на текущем Eigen-derived пуле. Не нетронутый upstream Eigen.'],
    EIGEN_SHARING:['sharing','Sharing: порт экспериментального partitioner коллег на текущем пуле. Не оригинальный исторический бинарник EgorkaZ.'],
    TBB_AUTO:['auto','Настоящий oneTBB Auto. Записано выполнение итераций; внутренних TBB-публикаций и краж в этой записи нет.'],
    RAPID_START:['mask','Сохранённый битмасочный прототип Rapid поверх oneTBB.'],
    RAPID_AUTO:['lazy','Rapid + Lazy: вор делит незарезервированный хвост работающего владельца. В учебной модели поиск бесплатен, а до первой резервации нового владельца другие workers получают ход; поэтому модель может показывать больше краж, чем реальная короткая запись.'],
    RAPID_NATIVE_AUTO:['native-auto','Rapid + Native Auto: обычный Auto после быстрого старта. Владелец публикует задачи, вор забирает их из очереди.'],
    RAPID_NATIVE_SIMPLE:['native-simple','Rapid + Native Simple: обычное рекурсивное деление до grain после быстрого старта.'],
    RAPID_NATIVE_STATIC:['native-static','Rapid + Native Static: пропорциональный префикс и фиксированное дальнейшее деление. Без Lazy-кражи хвоста.'],
    RAPID_NATIVE_AFFINITY:['native-affinity','Rapid + Native Affinity: предпочтения размещения и адаптивные обычные задачи. Модель не воспроизводит lifetime proxy и межвызовную историю.']
  };
  const records=JSON.parse(document.getElementById('trace-data').textContent);
  const select=document.getElementById('implementation');
  for(const r of records)if(pairs[r.mode])select.add(new Option(r.mode,r.mode));
  function show() {
    const record=records.find(r=>r.mode===select.value),notice=document.getElementById('provenance');
    if(!record){notice.textContent='Запусти сервер с --traces, чтобы загрузить реальные записи.';return;}
    const supported=record.tasks===64&&record.grain===2&&[4,8].includes(record.workers)&&['balanced','tail'].includes(record.workload);
    notice.textContent=pairs[record.mode][1]+' '+record.workers+' потоков, '+record.tasks+' итераций, grain='+record.grain+', '+record.workload+'. Учебная стоимость и накладные расходы не откалиброваны по этой записи.';
    document.getElementById('model-frame').hidden=!supported;
    if(supported)document.getElementById('model-frame').src='/?'+new URLSearchParams({model:pairs[record.mode][0],workers:record.workers,pattern:record.workload});
    else notice.textContent+=' Для этой записи нет модели с совпадающими параметрами; слева ничего не подставляется.';
    document.getElementById('trace-frame').src='/trace.html?'+new URLSearchParams({mode:record.mode});
  }
  select.addEventListener('change',show);show();
})();
