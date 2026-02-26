FROM php:8.5-cli-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends git unzip zip libzip-dev libonig-dev libicu-dev \
    && docker-php-ext-configure zip \
    && docker-php-ext-install -j"$(nproc)" intl mbstring pcntl zip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/local/bin/composer
COPY docker/php/conf.d/99-benchmark.ini /usr/local/etc/php/conf.d/99-benchmark.ini

WORKDIR /workspace

CMD ["bash"]
