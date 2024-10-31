'use client';

/* eslint-disable @typescript-eslint/no-explicit-any */
import {fetcher} from '@/api/api';

export function playSoundEffect(soundFile: any) {
  const context = new (window.AudioContext || (window as any).webkitAudioContext)();
  fetcher(soundFile, { responseType: 'arraybuffer' })
    .then(response => context.decodeAudioData(response))
    .then(audioBuffer => {
      const source = context.createBufferSource();
      source.buffer = audioBuffer;
      source.connect(context.destination);
      source.start();
    })
    .catch(e => console.log('error loading sound', e));
}