import { Module } from '@nestjs/common';
import { WagerProcessorService } from './wager-processor.service';
import { WageringController } from './wagering.controller';

@Module({
  controllers: [WageringController],
  providers: [WagerProcessorService],
  exports: [WagerProcessorService],
})
export class WageringModule {}
