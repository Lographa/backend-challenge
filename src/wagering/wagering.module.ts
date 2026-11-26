import { Module } from '@nestjs/common';
import { PendingReferenceWorker } from './pending-reference.worker';
import { WagerProcessorService } from './wager-processor.service';
import { WageringController } from './wagering.controller';

@Module({
  controllers: [WageringController],
  providers: [WagerProcessorService, PendingReferenceWorker],
  exports: [WagerProcessorService, PendingReferenceWorker],
})
export class WageringModule {}
