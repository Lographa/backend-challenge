import { type CanActivate, type ExecutionContext, Injectable, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

export const IS_PUBLIC_KEY = 'auth:isPublic';

/** Routes that stay open whatever authentication is plugged in (health checks). */
export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);

/**
 * Extension point for provider authentication, applied to every route. Authentication is not
 * implemented: every caller is accepted. A real implementation verifies here the provider
 * identity issued by an external IdP; public routes must keep bypassing it.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const isPublic = this.reflector.getAllAndOverride<boolean | undefined>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (isPublic === true) {
      return true;
    }
    return this.authenticate(context);
  }

  private authenticate(_context: ExecutionContext): boolean {
    return true;
  }
}
